package beacon

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/ethpandaops/dora/clients/consensus"
	"github.com/ethpandaops/dora/db"
	"github.com/ethpandaops/dora/dbtypes"
	"github.com/ethpandaops/dora/utils"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

type synchronizer struct {
	indexer *Indexer
	logger  logrus.FieldLogger

	syncCtx       context.Context
	syncCtxCancel context.CancelFunc
	runMutex      sync.Mutex

	stateMutex   sync.Mutex
	running      bool
	currentEpoch phase0.Epoch

	cachedSlot   phase0.Slot
	cachedBlocks map[phase0.Slot]*Block
}

func (indexer *Indexer) startSynchronizer(startEpoch phase0.Epoch) {
	if indexer.disableSync {
		return
	}
	if !indexer.synchronizer.isEpochAhead(startEpoch) || !indexer.synchronizer.running {
		indexer.synchronizer.startSync(startEpoch)
	}
}

func newSynchronizer(indexer *Indexer, logger logrus.FieldLogger) *synchronizer {
	sync := &synchronizer{
		indexer: indexer,
		logger:  logger,
	}

	// restore sync state
	syncState := &dbtypes.IndexerSyncState{}
	if _, err := db.GetExplorerState("indexer.syncstate", syncState); err == nil {
		sync.currentEpoch = phase0.Epoch(syncState.Epoch)
	}

	return sync
}

func (sync *synchronizer) isEpochAhead(epoch phase0.Epoch) bool {
	sync.stateMutex.Lock()
	defer sync.stateMutex.Unlock()
	if sync.running {
		if sync.currentEpoch < epoch {
			return true
		}
	}
	return false
}

func (sync *synchronizer) startSync(startEpoch phase0.Epoch) {
	sync.stopSync()

	// start synchronizer
	sync.stateMutex.Lock()
	defer sync.stateMutex.Unlock()
	if sync.running {
		sync.logger.Errorf("cannot start synchronizer: already running")
		return
	}
	if startEpoch < sync.currentEpoch {
		sync.currentEpoch = startEpoch
	}
	sync.running = true

	ctx, cancel := context.WithCancel(context.Background())
	sync.syncCtx = ctx
	sync.syncCtxCancel = cancel

	go sync.runSync()
}

func (s *synchronizer) stopSync() {
	var lockedMutex *sync.Mutex
	defer func() {
		if lockedMutex != nil {
			lockedMutex.Unlock()
		}
	}()

	s.stateMutex.Lock()
	lockedMutex = &s.stateMutex
	if s.running {
		s.syncCtxCancel()
	} else {
		return
	}
	s.stateMutex.Unlock()
	lockedMutex = nil

	s.runMutex.Lock()
	lockedMutex = &s.runMutex
}

func (sync *synchronizer) runSync() {
	defer utils.HandleSubroutinePanic("runSync", nil)

	sync.runMutex.Lock()
	defer sync.runMutex.Unlock()

	defer func() {
		sync.running = false
		sync.syncCtxCancel()
	}()

	sync.cachedBlocks = make(map[phase0.Slot]*Block)
	sync.cachedSlot = 0
	isComplete := false
	retryCount := 0

	sync.logger.Infof("synchronization started. head epoch: %v", sync.currentEpoch)

	for {
		// synchronize next epoch
		syncEpoch := sync.currentEpoch
		syncClients := sync.getSyncClients(syncEpoch)
		if len(syncClients) == 0 {
			sync.logger.Warnf("no clients available for synchronization of epoch %v", syncEpoch)

			// wait for 10 seconds before retrying
			time.Sleep(10 * time.Second)
			continue
		}

		if syncEpoch >= sync.indexer.lastFinalizedEpoch {
			isComplete = true
			break
		}

		retryLimit := len(syncClients)
		if retryLimit < 30 {
			retryLimit = 30
		}
		lastRetry := retryCount >= retryLimit
		syncClient := syncClients[retryCount%len(syncClients)]

		synclogger := sync.logger.WithFields(logrus.Fields{
			"epoch":  syncEpoch,
			"client": syncClient.client.GetName(),
		})

		if lastRetry {
			synclogger.Infof("synchronizing epoch %v (retry: %v, last retry!)", syncEpoch, retryCount)
		} else if retryCount > 0 {
			synclogger.Infof("synchronizing epoch %v (retry: %v)", syncEpoch, retryCount)
		} else {
			synclogger.Infof("synchronizing epoch %v", syncEpoch)
		}

		done, err := sync.syncEpoch(syncEpoch, syncClient, lastRetry)
		if done || lastRetry {
			if err != nil {
				sync.logger.Errorf("synchronization of epoch %v failed: %v - skipping epoch", syncEpoch, err)
			}
			retryCount = 0
			sync.stateMutex.Lock()
			syncEpoch++
			sync.currentEpoch = syncEpoch
			sync.stateMutex.Unlock()
			if syncEpoch >= sync.indexer.lastFinalizedEpoch {
				isComplete = true
				break
			}
		} else if err != nil {
			synclogger.Warnf("synchronization of epoch %v failed: %v - Retrying in 10 sec...", syncEpoch, err)
			retryCount++
			time.Sleep(10 * time.Second)
		}

		if sync.syncCtx.Err() != nil {
			break
		}
	}

	if isComplete {
		sync.logger.Infof("synchronization complete. Head epoch: %v", sync.currentEpoch)
		db.RunDBTransaction(func(tx *sqlx.Tx) error {
			return db.SetExplorerState("indexer.syncstate", &dbtypes.IndexerSyncState{
				Epoch: uint64(sync.currentEpoch),
			}, tx)
		})
	} else {
		sync.logger.Infof("synchronization aborted. Head epoch: %v", sync.currentEpoch)
	}

	sync.running = false
}

func (sync *synchronizer) getSyncClients(epoch phase0.Epoch) []*Client {
	archiveClients := make([]*Client, 0)
	normalClients := make([]*Client, 0)

	for _, client := range sync.indexer.clients {
		if client.client.GetStatus() != consensus.ClientStatusOnline {
			continue
		}

		if client.skipValidators {
			continue
		}

		finalizedEpoch, _, _, _ := client.client.GetFinalityCheckpoint()
		if finalizedEpoch < epoch {
			continue
		}

		if client.archive {
			archiveClients = append(archiveClients, client)
		} else {
			normalClients = append(normalClients, client)
		}
	}

	sort.Slice(archiveClients, func(i, j int) bool {
		if archiveClients[i].priority == archiveClients[j].priority {
			return rand.UintN(1) == 0
		}
		return archiveClients[i].priority > archiveClients[j].priority
	})

	sort.Slice(normalClients, func(i, j int) bool {
		if normalClients[i].priority == normalClients[j].priority {
			return rand.UintN(1) == 0
		}
		return normalClients[i].priority > normalClients[j].priority
	})

	return append(archiveClients, normalClients...)
}

func (sync *synchronizer) loadBlockHeader(client *Client, slot phase0.Slot) (*phase0.SignedBeaconBlockHeader, phase0.Root, error) {
	ctx, cancel := context.WithTimeout(sync.syncCtx, beaconHeaderRequestTimeout)
	defer cancel()

	header, root, orphaned, err := LoadBeaconHeaderBySlot(ctx, client, slot)
	if orphaned {
		return nil, root, nil
	}

	return header, root, err
}

func (sync *synchronizer) loadBlockBody(client *Client, root phase0.Root) (*spec.VersionedSignedBeaconBlock, error) {
	ctx, cancel := context.WithTimeout(sync.syncCtx, beaconHeaderRequestTimeout)
	defer cancel()
	return LoadBeaconBlock(ctx, client, root)
}

func (sync *synchronizer) syncEpoch(syncEpoch phase0.Epoch, client *Client, lastTry bool) (bool, error) {
	if !utils.Config.Indexer.ResyncForceUpdate && db.IsEpochSynchronized(uint64(syncEpoch)) {
		return true, nil
	}

	chainState := sync.indexer.consensusPool.GetChainState()
	specs := chainState.GetSpecs()

	// load headers & blocks from this & next epoch
	firstSlot := chainState.EpochStartSlot(syncEpoch)
	lastSlot := chainState.EpochStartSlot(syncEpoch+2) - 1
	canonicalBlocks := []*Block{}
	canonicalBlockRoots := [][]byte{}
	canonicalBlockHashes := [][]byte{}
	nextEpochCanonicalBlocks := []*Block{}

	var firstBlock *Block
	for slot := firstSlot; slot <= lastSlot; slot++ {
		if sync.cachedSlot < slot || sync.cachedBlocks[slot] == nil {
			blockHeader, blockRoot, err := sync.loadBlockHeader(client, slot)
			if err != nil {
				return false, fmt.Errorf("error fetching slot %v header: %v", slot, err)
			}
			if blockHeader == nil {
				continue
			}
			if sync.syncCtx.Err() != nil {
				return false, nil
			}

			block := newBlock(sync.indexer.dynSsz, blockRoot, slot)
			block.SetHeader(blockHeader)

			if slot > 0 {
				blockBody, err := sync.loadBlockBody(client, phase0.Root(blockRoot))
				if err != nil {
					return false, fmt.Errorf("error fetching slot %v block: %v", slot, err)
				}
				if blockBody == nil {
					return false, fmt.Errorf("error fetching slot %v block: not found", slot)
				}

				block.SetBlock(blockBody)
			}

			sync.cachedBlocks[slot] = block
		}

		if firstBlock == nil && sync.cachedBlocks[slot] != nil {
			firstBlock = sync.cachedBlocks[slot]
		}

		if chainState.EpochOfSlot(slot) == syncEpoch {
			canonicalBlocks = append(canonicalBlocks, sync.cachedBlocks[slot])
			canonicalBlockRoots = append(canonicalBlockRoots, sync.cachedBlocks[slot].Root[:])
			if blockIndex := sync.cachedBlocks[slot].GetBlockIndex(); blockIndex != nil {
				canonicalBlockHashes = append(canonicalBlockHashes, blockIndex.ExecutionHash[:])
			}
		} else {
			nextEpochCanonicalBlocks = append(nextEpochCanonicalBlocks, sync.cachedBlocks[slot])
		}
	}
	sync.cachedSlot = lastSlot

	if sync.syncCtx.Err() != nil {
		return false, nil
	}

	// load epoch state
	var dependentRoot phase0.Root
	if firstBlock != nil {
		if firstBlock.Slot == 0 { // epoch 0 dependent root is the genesis block
			dependentRoot = firstBlock.Root
		} else {
			dependentRoot = firstBlock.header.Message.ParentRoot
		}
	} else {
		// get from db
		depRoot := db.GetHighestRootBeforeSlot(uint64(firstSlot), false)
		dependentRoot = phase0.Root(depRoot)
	}

	epochState := newEpochState(dependentRoot)
	state, err := epochState.loadState(sync.syncCtx, client, nil)
	if (err != nil || epochState.loadingStatus != 2) && !lastTry {
		return false, fmt.Errorf("error fetching epoch %v state: %v", syncEpoch, err)
	}

	var epochStats *EpochStats
	var epochStatsValues *EpochStatsValues
	if epochState != nil && epochState.loadingStatus == 2 {
		epochStats = newEpochStats(syncEpoch, dependentRoot)
		epochStats.dependentState = epochState

		var validatorSet []*phase0.Validator
		if state == nil {
			sync.logger.Warnf("state for epoch %v not found", syncEpoch)
		} else {
			validatorSet, err = state.Validators()
			if err != nil {
				sync.logger.Warnf("error getting validator set from state %v: %v", epochStats.dependentRoot.String(), err)
			}
		}

		epochStats.processState(sync.indexer, validatorSet)
		epochStatsValues = epochStats.GetValues(false)
	}

	if sync.syncCtx.Err() != nil {
		return false, nil
	}

	// process epoch vote aggregations
	var epochVotes *EpochVotes
	if epochStatsValues != nil {
		votingBlocks := make([]*Block, len(canonicalBlocks)+len(nextEpochCanonicalBlocks))
		copy(votingBlocks, canonicalBlocks)
		copy(votingBlocks[len(canonicalBlocks):], nextEpochCanonicalBlocks)
		epochVotes = sync.indexer.aggregateEpochVotes(syncEpoch, chainState, votingBlocks, epochStats)
		if epochVotes == nil && !lastTry {
			return false, fmt.Errorf("failed computing votes for epoch %v", syncEpoch)
		}
	}

	// save blocks
	err = db.RunDBTransaction(func(tx *sqlx.Tx) error {
		err = sync.indexer.dbWriter.persistEpochData(tx, syncEpoch, canonicalBlocks, epochStats, epochVotes)
		if err != nil {
			return fmt.Errorf("error persisting epoch data to db: %v", err)
		}

		// persist sync committee assignments
		if err := sync.indexer.dbWriter.persistSyncAssignments(tx, syncEpoch, epochStats); err != nil {
			return fmt.Errorf("error persisting sync committee assignments to db: %v", err)
		}

		if err := db.UpdateMevBlockByEpoch(uint64(syncEpoch), specs.SlotsPerEpoch, canonicalBlockHashes, tx); err != nil {
			return fmt.Errorf("error while updating mev block proposal state: %v", err)
		}

		// delete unfinalized epoch aggregations in epoch
		if err := db.DeleteUnfinalizedEpochsBefore(uint64(syncEpoch+1), tx); err != nil {
			return fmt.Errorf("failed deleting unfinalized epoch aggregations <= epoch %v: %v", syncEpoch, err)
		}

		// delete unfinalized forks for canonical roots
		if len(canonicalBlockRoots) > 0 {
			if err := db.UpdateFinalizedForkParents(canonicalBlockRoots, tx); err != nil {
				return fmt.Errorf("failed updating finalized fork parents: %v", err)
			}
			if err := db.DeleteFinalizedForks(canonicalBlockRoots, tx); err != nil {
				return fmt.Errorf("failed deleting finalized forks: %v", err)
			}
		}

		err = db.SetExplorerState("indexer.syncstate", &dbtypes.IndexerSyncState{
			Epoch: uint64(syncEpoch),
		}, tx)
		if err != nil {
			return fmt.Errorf("error while updating sync state: %v", err)
		}

		return nil
	})
	if err != nil {
		return false, err
	}

	// cleanup cache (remove blocks from this epoch)
	for slot := range sync.cachedBlocks {
		if slot <= lastSlot {
			delete(sync.cachedBlocks, slot)
		}
	}

	return true, nil
}
