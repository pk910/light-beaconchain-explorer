package beacon

import (
	"context"
	"fmt"
	"sort"
	"time"

	v1 "github.com/attestantio/go-eth2-client/api/v1"
	"github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/ethpandaops/dora/db"
	"github.com/ethpandaops/dora/dbtypes"
	"github.com/jmoiron/sqlx"
)

// processFinalityEvent processes a finality event.
func (indexer *Indexer) processFinalityEvent(finalityEvent *v1.Finality) error {
	// first wait 5 seconds for other clients to process the finality checkpoint
	time.Sleep(5 * time.Second)

	indexer.logger.Infof("process finality event (epoch: %v, root: %v)", finalityEvent.Finalized.Epoch, finalityEvent.Finalized.Root.String())
	startSynchronizer := false
	synchronizeFromEpoch := phase0.Epoch(0)
	oldLastFinalizedEpoch := indexer.lastFinalizedEpoch

	for finalizeEpoch := indexer.lastFinalizedEpoch; finalizeEpoch < finalityEvent.Finalized.Epoch; finalizeEpoch++ {
		readyClients := indexer.GetReadyClientsByCheckpoint(finalityEvent.Finalized.Root, true)
		retryCount := 5

		for {
			if len(readyClients) > 0 {
				break
			}

			if retryCount > 0 {
				indexer.logger.Warnf("no ready clients for epoch %v finalization, retrying in 5 sec...", finalizeEpoch)
				time.Sleep(5 * time.Second)
				retryCount--
			} else {
				indexer.logger.Warnf("no ready clients for epoch %v finalization", finalizeEpoch)
				break
			}
		}

		retryCount = len(readyClients)
		if retryCount == 0 {
			indexer.logger.Infof("need synchronization! epoch %v, reason: no clients", finalizeEpoch)
			if !startSynchronizer {
				synchronizeFromEpoch = finalizeEpoch
				startSynchronizer = true
			}
			indexer.lastFinalizedEpoch = finalizeEpoch + 1
			break
		}

		clientIdx := 0

		indexer.logger.Infof("finalizing epoch %v", finalizeEpoch)
		var finalizationError error
		for retry := 0; retry < retryCount; retry++ {
			lastTry := retry == retryCount-1
			client := readyClients[clientIdx%len(readyClients)]

			canRetry, err := indexer.finalizeEpoch(finalizeEpoch, finalityEvent.Justified.Root, client, lastTry)
			finalizationError = err
			if err != nil {
				if !lastTry && canRetry {
					indexer.logger.WithError(finalizationError).Warnf("failed finalizing epoch %v, retrying...", finalizeEpoch)
					time.Sleep(10 * time.Second)
				} else {
					indexer.logger.WithError(finalizationError).Errorf("failed finalizing epoch %v", finalizeEpoch)
				}

				if canRetry {
					continue
				} else {
					break
				}
			} else if canRetry {
				// finalization processing is not complete, still needs resync
				indexer.logger.Infof("need synchronization! epoch %v, reason: incomplete", finalizeEpoch)
				if !startSynchronizer {
					synchronizeFromEpoch = finalizeEpoch
					startSynchronizer = true
				}
			}

			break
		}

		if finalizationError != nil {
			indexer.logger.Infof("need synchronization! epoch %v, reason: error", finalizeEpoch)
			if !startSynchronizer {
				synchronizeFromEpoch = finalizeEpoch
				startSynchronizer = true
			}
		}
	}

	if startSynchronizer {
		indexer.startSynchronizer(synchronizeFromEpoch)
	} else if !indexer.synchronizer.running && indexer.synchronizer.currentEpoch >= oldLastFinalizedEpoch && indexer.lastFinalizedEpoch > oldLastFinalizedEpoch {
		indexer.synchronizer.currentEpoch = indexer.lastFinalizedEpoch
		err := db.RunDBTransaction(func(tx *sqlx.Tx) error {
			return db.SetExplorerState("indexer.syncstate", &dbtypes.IndexerSyncState{
				Epoch: uint64(indexer.lastFinalizedEpoch),
			}, tx)
		})
		if err != nil {
			indexer.logger.WithError(err).Errorf("failed updating sync state")
		}
	}

	return nil
}

// finalizeEpoch finalizes the epoch by persisting the epoch data (incl. blocks & aggregations) to the database.
func (indexer *Indexer) finalizeEpoch(epoch phase0.Epoch, justifiedRoot phase0.Root, client *Client, lastTry bool) (bool, error) {
	t1 := time.Now()
	t1loading := time.Duration(0)
	epochBlocks := indexer.blockCache.getEpochBlocks(epoch)
	nextEpochBlocks := indexer.blockCache.getEpochBlocks(epoch + 1)
	chainState := indexer.consensusPool.GetChainState()
	specs := chainState.GetSpecs()

	canonicalBlocks := []*Block{}
	orphanedBlocks := []*Block{}
	nextEpochCanonicalBlocks := []*Block{}

	var dependentRoot phase0.Root

	for _, block := range epochBlocks {
		// restore block body from db as we gonna use it a lot for the epoch & voting aggregations
		// block is wiped from cache after processing anyway, so no need to prune it again
		block.unpruneBlockBody()

		if indexer.blockCache.isCanonicalBlock(block.Root, justifiedRoot) {
			if _, err := block.EnsureBlock(func() (*spec.VersionedSignedBeaconBlock, error) {
				return LoadBeaconBlock(client.getContext(), client, block.Root)
			}); err != nil {
				client.logger.Warnf("failed loading finalized block body %v (%v): %v", block.Slot, block.Root.String(), err)
			}

			if block.block == nil {
				return true, fmt.Errorf("missing block body for canonical block %v (%v)", block.Slot, block.Root.String())
			}
			canonicalBlocks = append(canonicalBlocks, block)
		} else {
			if block.block == nil {
				indexer.logger.Warnf("missing block body for orphaned block %v (%v)", block.Slot, block.Root.String())
				continue
			}

			orphanedBlocks = append(orphanedBlocks, block)
		}
	}

	for _, block := range nextEpochBlocks {
		if indexer.blockCache.isCanonicalBlock(block.Root, justifiedRoot) {
			block.unpruneBlockBody()
			nextEpochCanonicalBlocks = append(nextEpochCanonicalBlocks, block)
		}
	}

	// sort by slot, all aggregations expect blocks in ascending order
	sort.Slice(canonicalBlocks, func(i, j int) bool {
		return canonicalBlocks[i].Slot < canonicalBlocks[j].Slot
	})
	sort.Slice(nextEpochCanonicalBlocks, func(i, j int) bool {
		return nextEpochCanonicalBlocks[i].Slot < nextEpochCanonicalBlocks[j].Slot
	})

	// check if first canonical block is really the first block of the epoch
	// clients do backfilling, so we only need to check if the first block
	if len(canonicalBlocks) > 0 {
		// check if first blocks parent is from parent epoch
		firstBlock := canonicalBlocks[0]
		isValid := false

		dependentBlock := indexer.blockCache.getDependentBlock(chainState, firstBlock, client)
		if dependentBlock != nil {
			dependentRoot = dependentBlock.Root
			isValid = chainState.EpochOfSlot(dependentBlock.Slot) < chainState.EpochOfSlot(firstBlock.Slot) || dependentBlock.Slot == 0
		} else {
			depRoot := firstBlock.GetParentRoot()
			if depRoot != nil {
				dependentRoot = *depRoot
				dependentHead, _ := LoadBeaconHeader(client.getContext(), client, *depRoot)
				isValid = dependentHead != nil && chainState.EpochOfSlot(dependentHead.Message.Slot) < chainState.EpochOfSlot(firstBlock.Slot)
			}
		}

		if !isValid {
			return false, fmt.Errorf("first canonical block %v (%v) is not the first block of epoch %v", firstBlock.Slot, firstBlock.Root.String(), epoch)
		}
	} else {
		// check if there's really no canonical block in the epoch
		canonicalBlock := indexer.blockCache.getBlockByRoot(justifiedRoot)
		for {
			if canonicalBlock == nil {
				return false, fmt.Errorf("missing blocks between epoch %v and the finalized checkpoint", epoch)
			}

			blockEpoch := chainState.EpochOfSlot(canonicalBlock.Slot)
			if blockEpoch == epoch {
				return false, fmt.Errorf("missing blocks in epoch %v", epoch)
			}

			if chainState.EpochOfSlot(canonicalBlock.Slot) < epoch {
				// we've walked back to the previous epoch without finding any canonical block for this epoch
				// so there's no canonical block in this epoch
				dependentRoot = canonicalBlock.Root
				break
			}

			parentRoot := canonicalBlock.GetParentRoot()
			if parentRoot == nil {
				return false, fmt.Errorf("missing blocks between epoch %v and the finalized checkpoint", epoch)
			}

			canonicalBlock = indexer.blockCache.getBlockByRoot(*parentRoot)
			if canonicalBlock == nil {
				blockHead := db.GetBlockHeadByRoot((*parentRoot)[:])
				if blockHead != nil {
					canonicalBlock = newBlock(indexer.dynSsz, phase0.Root(blockHead.Root), phase0.Slot(blockHead.Slot))
					canonicalBlock.isInFinalizedDb = true
					parentRootVal := phase0.Root(blockHead.ParentRoot)
					canonicalBlock.parentRoot = &parentRootVal
				}
			}
			if canonicalBlock == nil {
				dependentHead, _ := LoadBeaconHeader(client.getContext(), client, *parentRoot)

				if dependentHead != nil {
					canonicalBlock = newBlock(indexer.dynSsz, phase0.Root(*parentRoot), phase0.Slot(dependentHead.Message.Slot))
					canonicalBlock.isInFinalizedDb = true
					parentRootVal := phase0.Root(dependentHead.Message.ParentRoot)
					canonicalBlock.parentRoot = &parentRootVal
				}
			}
		}
	}

	// get epoch stats
	var epochStatsValues *EpochStatsValues
	var epochVotes *EpochVotes

	epochStats := indexer.epochCache.getEpochStats(epoch, dependentRoot)

	if epochStats != nil {
		// ensure epoch stats are loaded
		// if the state is not yet loaded, we set it to high priority and wait for it to be loaded
		if !epochStats.ready {
			if epochStats.dependentState == nil {
				indexer.epochCache.addEpochStateRequest(epochStats)
			}
			if epochStats.dependentState != nil && epochStats.dependentState.loadingStatus != 2 && epochStats.dependentState.retryCount < 10 {
				indexer.logger.Infof("epoch %d state (%v) not yet loaded, waiting for state to be loaded", epoch, dependentRoot.String())
				t1 := time.Now()
				epochStats.dependentState.highPriority = true
				loaded := epochStats.dependentState.awaitStateLoaded(context.Background(), beaconStateRequestTimeout)
				if loaded {
					// wait for async duty computation to be completed
					epochStats.awaitStatsReady(context.Background(), 30*time.Second)
				}
				t1loading += time.Since(t1)
			}
		}

		epochStatsValues = epochStats.GetOrLoadValues(indexer, false, true)
	}

	if epochStatsValues == nil {
		if !lastTry { // do not error on last try, we can at least persist the canonical and orphaned blocks even without epoch stats
			return false, fmt.Errorf("missing epoch stats values for epoch %v", epoch)
		}
	} else {
		// compute votes for canonical blocks
		votingBlocks := make([]*Block, len(canonicalBlocks)+len(nextEpochCanonicalBlocks))
		copy(votingBlocks, canonicalBlocks)
		copy(votingBlocks[len(canonicalBlocks):], nextEpochCanonicalBlocks)
		epochVotes = indexer.aggregateEpochVotes(epoch, chainState, votingBlocks, epochStats)
		if epochVotes == nil && !lastTry {
			return false, fmt.Errorf("failed computing votes for epoch %v", epoch)
		}
	}

	canonicalRoots := make([][]byte, len(canonicalBlocks))
	canonicalBlockHashes := make([][]byte, len(canonicalBlocks))
	for i, block := range canonicalBlocks {
		canonicalRoots[i] = block.Root[:]
		if blockIndex := block.GetBlockIndex(); blockIndex != nil {
			canonicalBlockHashes[i] = blockIndex.ExecutionHash[:]
		}
	}

	t1dur := time.Since(t1) - t1loading
	t1 = time.Now()

	// persist to db
	deleteBeforeSlot := chainState.EpochToSlot(epoch + 1)
	err := db.RunDBTransaction(func(tx *sqlx.Tx) error {
		// persist canonical epoch data
		if err := indexer.dbWriter.persistEpochData(tx, epoch, canonicalBlocks, epochStats, epochVotes); err != nil {
			return fmt.Errorf("failed persisting epoch data for epoch %v: %v", epoch, err)
		}

		// persist orphaned blocks
		for _, block := range orphanedBlocks {
			dependentBlock := indexer.blockCache.getDependentBlock(chainState, block, client)

			var epochStats *EpochStats
			if dependentBlock != nil {
				epochStats = indexer.epochCache.getEpochStats(epoch, dependentBlock.Root)
			}

			if _, err := indexer.dbWriter.persistBlockData(tx, block, epochStats, nil, true, nil); err != nil {
				return fmt.Errorf("failed persisting orphaned slot %v (%v): %v", block.Slot, block.Root.String(), err)
			}

			orphanedBlock, err := block.buildOrphanedBlock(indexer.blockCompression)
			if err != nil {
				return fmt.Errorf("failed building orphaned block %v (%v): %v", block.Slot, block.Root.String(), err)
			}

			if err := db.InsertOrphanedBlock(orphanedBlock, tx); err != nil {
				return fmt.Errorf("failed persisting orphaned slot %v (%v): %v", block.Slot, block.Root.String(), err)
			}
		}

		// persist sync committee assignments
		if err := indexer.dbWriter.persistSyncAssignments(tx, epoch, epochStats); err != nil {
			return fmt.Errorf("error persisting sync committee assignments to db: %v", err)
		}

		if err := db.UpdateMevBlockByEpoch(uint64(epoch), specs.SlotsPerEpoch, canonicalBlockHashes, tx); err != nil {
			return fmt.Errorf("error while updating mev block proposal state: %v", err)
		}

		// delete unfinalized duties before epoch
		if err := db.DeleteUnfinalizedDutiesBefore(uint64(epoch+1), tx); err != nil {
			return fmt.Errorf("failed deleting unfinalized duties <= epoch %v: %v", epoch, err)
		}

		// delete unfinalized blocks before epoch
		if err := db.DeleteUnfinalizedBlocksBefore(uint64(deleteBeforeSlot), tx); err != nil {
			return fmt.Errorf("failed deleting unfinalized duties < slot %v: %v", deleteBeforeSlot, err)
		}

		// delete unfinalized epoch aggregations in epoch
		if err := db.DeleteUnfinalizedEpochsBefore(uint64(epoch+1), tx); err != nil {
			return fmt.Errorf("failed deleting unfinalized epoch aggregations <= epoch %v: %v", epoch, err)
		}

		// delete unfinalized forks for canonical roots
		if len(canonicalRoots) > 0 {
			if err := db.UpdateFinalizedForkParents(canonicalRoots, tx); err != nil {
				return fmt.Errorf("failed updating finalized fork parents: %v", err)
			}
			if err := db.DeleteFinalizedForks(canonicalRoots, tx); err != nil {
				return fmt.Errorf("failed deleting finalized forks: %v", err)
			}
		}

		return nil
	})
	if err != nil {
		return false, fmt.Errorf("failed persisting epoch %v data: %v", epoch, err)
	}

	t2dur := time.Since(t1)

	indexer.lastFinalizedEpoch = epoch + 1

	// sleep 500 ms to give running UI threads time to fetch data from cache
	time.Sleep(500 * time.Millisecond)

	t1 = time.Now()

	// update validator cache
	if len(canonicalBlocks) > 0 {
		indexer.validatorCache.setFinalizedEpoch(epoch, canonicalBlocks[len(canonicalBlocks)-1].Root)
	}

	// clean fork cache
	indexer.forkCache.setFinalizedEpoch(deleteBeforeSlot, justifiedRoot)
	for _, fork := range indexer.forkCache.getForksBefore(deleteBeforeSlot) {
		indexer.forkCache.removeFork(fork.forkId)
	}

	// clean epoch stats
	indexer.epochCache.removeEpochStatsByEpoch(epoch)

	// clean block cache
	for _, block := range canonicalBlocks {
		indexer.blockCache.removeBlock(block)
	}
	for _, block := range orphanedBlocks {
		indexer.blockCache.removeBlock(block)
	}

	t3dur := time.Since(t1)

	indexer.metrics.finalizationLoadDuration.Observe(float64(t1loading.Milliseconds()))
	indexer.metrics.finalizationProcessDuration.Observe(float64(t1dur.Milliseconds()))
	indexer.metrics.finalizationStoreDuration.Observe(float64(t2dur.Milliseconds()))
	indexer.metrics.finalizationCleanDuration.Observe(float64(t3dur.Milliseconds()))

	// log summary
	indexer.logger.Infof("completed epoch %v finalization (process: %v ms, load: %v s, write: %v ms, clean: %v ms)", epoch, t1dur.Milliseconds(), t1loading.Seconds(), t2dur.Milliseconds(), t3dur.Milliseconds())
	indexer.logger.Infof("epoch %v blocks: %v canonical, %v orphaned", epoch, len(canonicalBlocks), len(orphanedBlocks))
	if epochStatsValues != nil {
		indexer.logger.Infof("epoch %v stats: %v validators (%v ETH)", epoch, epochStatsValues.ActiveValidators, epochStatsValues.EffectiveBalance/EtherGweiFactor)
		indexer.logger.Infof(
			"epoch %v votes: target %v + %v = %v ETH (%.2f%%)",
			epoch,
			epochVotes.CurrentEpoch.TargetVoteAmount/EtherGweiFactor,
			epochVotes.NextEpoch.TargetVoteAmount/EtherGweiFactor,
			(epochVotes.CurrentEpoch.TargetVoteAmount+epochVotes.NextEpoch.TargetVoteAmount)/EtherGweiFactor,
			epochVotes.TargetVotePercent,
		)
		indexer.logger.Infof(
			"epoch %v votes: head %v + %v = %v ETH (%.2f%%)",
			epoch,
			epochVotes.CurrentEpoch.HeadVoteAmount/EtherGweiFactor,
			epochVotes.NextEpoch.HeadVoteAmount/EtherGweiFactor,
			(epochVotes.CurrentEpoch.HeadVoteAmount+epochVotes.NextEpoch.HeadVoteAmount)/EtherGweiFactor,
			epochVotes.HeadVotePercent,
		)
		indexer.logger.Infof(
			"epoch %v votes: total %v + %v = %v ETH (%.2f%%)",
			epoch,
			epochVotes.CurrentEpoch.TotalVoteAmount/EtherGweiFactor,
			epochVotes.NextEpoch.TotalVoteAmount/EtherGweiFactor,
			(epochVotes.CurrentEpoch.TotalVoteAmount+epochVotes.NextEpoch.TotalVoteAmount)/EtherGweiFactor,
			epochVotes.TotalVotePercent,
		)
	}

	return (epochStatsValues == nil || epochVotes == nil), nil
}
