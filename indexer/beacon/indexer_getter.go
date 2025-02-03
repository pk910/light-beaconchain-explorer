package beacon

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"

	v1 "github.com/attestantio/go-eth2-client/api/v1"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/ethpandaops/dora/clients/consensus"
	"github.com/ethpandaops/dora/db"
)

// GetAllClients returns a slice of all clients in the indexer.
func (indexer *Indexer) GetAllClients() []*Client {
	clients := make([]*Client, len(indexer.clients))
	copy(clients, indexer.clients)
	return clients
}

// GetReadyClientsByCheckpoint returns a slice of clients that are ready for processing based on the finalized root and preference for archive clients.
func (indexer *Indexer) GetReadyClientsByCheckpoint(finalizedRoot phase0.Root, preferArchive bool) []*Client {
	clients := make([]*Client, 0)

	for _, client := range indexer.clients {
		if client.client.GetStatus() != consensus.ClientStatusOnline {
			continue
		}

		_, root, _, _ := client.client.GetFinalityCheckpoint()
		if !bytes.Equal(root[:], finalizedRoot[:]) && !bytes.Equal(root[:], consensus.NullRoot[:]) {
			continue
		}

		clients = append(clients, client)
	}

	sort.Slice(clients, func(i, j int) bool {
		if preferArchive && clients[i].archive != clients[j].archive {
			return clients[i].archive
		}

		if clients[i].priority != clients[j].priority {
			return clients[i].priority > clients[j].priority
		}

		return rand.IntN(2) == 0
	})

	return clients
}

// GetReadyClientsByBlockRoot returns a slice of clients that are ready for requests for the chain including the block root and preference for archive clients.
func (indexer *Indexer) GetReadyClientsByBlockRoot(blockRoot phase0.Root, preferArchive bool) []*Client {
	clients := make([]*Client, 0)

	for _, client := range indexer.clients {
		if client.client.GetStatus() != consensus.ClientStatusOnline {
			continue
		}

		_, root := client.client.GetLastHead()
		if indexer.blockCache.isCanonicalBlock(blockRoot, root) {
			clients = append(clients, client)
		}
	}

	sort.Slice(clients, func(i, j int) bool {
		if preferArchive && clients[i].archive != clients[j].archive {
			return clients[i].archive
		}

		if clients[i].priority != clients[j].priority {
			return clients[i].priority > clients[j].priority
		}

		return rand.IntN(2) == 0
	})

	return clients
}

// GetReadyClientByBlockRoot returns a single client that is ready for requests for the chain including the block root and preference for archive clients.
func (indexer *Indexer) GetReadyClientByBlockRoot(blockRoot phase0.Root, preferArchive bool) *Client {
	clients := indexer.GetReadyClientsByBlockRoot(blockRoot, preferArchive)
	if len(clients) > 0 {
		return clients[0]
	}

	return nil
}

// GetReadyClients returns a slice of clients that are on the finalized chain and preference for archive clients.
func (indexer *Indexer) GetReadyClients(preferArchive bool) []*Client {
	_, finalizedRoot := indexer.consensusPool.GetChainState().GetFinalizedCheckpoint()
	clients := indexer.GetReadyClientsByCheckpoint(finalizedRoot, preferArchive)
	if len(clients) == 0 {
		clients = indexer.GetReadyClientsByCheckpoint(consensus.NullRoot, preferArchive)
	}
	return clients
}

// GetReadyClient returns a single client that is on the finalized chain and preference for archive clients.
func (indexer *Indexer) GetReadyClient(preferArchive bool) *Client {
	clients := indexer.GetReadyClients(preferArchive)
	if len(clients) > 0 {
		return clients[0]
	}

	return nil
}

// GetBlockCacheState returns the state of the block cache, including the last finalized epoch and the last pruned epoch.
// this represents the internal cache state and might be behind the actual finalization checkpoint.
func (indexer *Indexer) GetBlockCacheState() (finalizedEpoch phase0.Epoch, prunedEpoch phase0.Epoch) {
	return indexer.lastFinalizedEpoch, indexer.lastPrunedEpoch
}

// GetSynchronizerState returns the state of the synchronizer, including whether it is running and the current epoch.
func (indexer *Indexer) GetSynchronizerState() (running bool, syncHead phase0.Epoch) {
	if indexer.synchronizer == nil {
		return false, 0
	}

	return indexer.synchronizer.running, indexer.synchronizer.currentEpoch
}

// GetForkHeads returns a slice of fork heads in the indexer.
func (indexer *Indexer) GetForkHeads() []*ForkHead {
	return indexer.forkCache.getForkHeads()
}

// GetBlockByRoot returns the block with the given block root.
func (indexer *Indexer) GetBlockByRoot(blockRoot phase0.Root) *Block {
	return indexer.blockCache.getBlockByRoot(blockRoot)
}

// GetBlocksBySlot returns a slice of blocks with the given slot.
func (indexer *Indexer) GetBlocksBySlot(slot phase0.Slot) []*Block {
	return indexer.blockCache.getBlocksBySlot(slot)
}

// GetBlockByParentRoot returns a slice of blocks with the given parent root.
func (indexer *Indexer) GetBlockByParentRoot(blockRoot phase0.Root) []*Block {
	return indexer.blockCache.getBlocksByParentRoot(blockRoot)
}

// GetBlockByStateRoot returns the block with the given state root.
func (indexer *Indexer) GetBlockByStateRoot(stateRoot phase0.Root) *Block {
	return indexer.blockCache.getBlockByStateRoot(stateRoot)
}

// GetBlocksByExecutionBlockHash returns a slice of blocks with the given execution block hash.
func (indexer *Indexer) GetBlocksByExecutionBlockHash(blockHash phase0.Hash32) []*Block {
	return indexer.blockCache.getBlocksByExecutionBlockHash(blockHash)
}

// GetBlocksByExecutionBlockNumber returns a slice of blocks with the given execution block number.
func (indexer *Indexer) GetBlocksByExecutionBlockNumber(blockNumber uint64) []*Block {
	return indexer.blockCache.getBlocksByExecutionBlockNumber(blockNumber)
}

// GetBlockDistance returns whether the base root is in the canonical chain defined by the head root and the distance between both blocks.
func (indexer *Indexer) GetBlockDistance(baseRoot phase0.Root, headRoot phase0.Root) (bool, uint64) {
	return indexer.blockCache.getCanonicalDistance(baseRoot, headRoot, 0)
}

// GetOrphanedBlockByRoot returns the orphaned block with the given block root.
func (indexer *Indexer) GetOrphanedBlockByRoot(blockRoot phase0.Root) (*Block, error) {
	orphanedBlock := db.GetOrphanedBlock(blockRoot[:])
	if orphanedBlock == nil {
		return nil, nil
	}

	if orphanedBlock.HeaderVer != 1 {
		return nil, fmt.Errorf("failed unmarshal orphaned block header [%x] from db: unsupported header version", orphanedBlock.Root)
	}

	header := &phase0.SignedBeaconBlockHeader{}
	err := header.UnmarshalSSZ(orphanedBlock.HeaderSSZ)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshal orphaned block header [%x] from db: %v", orphanedBlock.Root, err)
	}

	blockBody, err := unmarshalVersionedSignedBeaconBlockSSZ(indexer.dynSsz, orphanedBlock.BlockVer, orphanedBlock.BlockSSZ)
	if err != nil {
		return nil, fmt.Errorf("could not restore orphaned block body %v [%x] from db: %v", header.Message.Slot, orphanedBlock.Root, err)
	}

	block := newBlock(indexer.dynSsz, blockRoot, header.Message.Slot)
	block.SetHeader(header)
	block.SetBlock(blockBody)

	return block, nil
}

// GetEpochStats returns the epoch stats for the given epoch and optional fork ID override.
func (indexer *Indexer) GetEpochStats(epoch phase0.Epoch, overrideForkId *ForkKey) *EpochStats {
	epochStats := indexer.epochCache.getEpochStatsByEpoch(epoch)
	if len(epochStats) == 0 {
		return nil
	}

	if len(epochStats) == 1 {
		return epochStats[0]
	}

	canonicalHead := indexer.GetCanonicalHead(overrideForkId)

	var bestEpochStats *EpochStats
	var bestDistance phase0.Slot

	if canonicalHead != nil {
		canonicalForkIds := indexer.forkCache.getParentForkIds(canonicalHead.forkId)

		for _, stats := range epochStats {
			if !stats.ready {
				continue
			}

			dependentBlock := indexer.blockCache.getBlockByRoot(stats.dependentRoot)
			if dependentBlock == nil {
				blockHead := db.GetBlockHeadByRoot(stats.dependentRoot[:])
				if blockHead != nil {
					dependentBlock = newBlock(indexer.dynSsz, phase0.Root(blockHead.Root), phase0.Slot(blockHead.Slot))
					dependentBlock.isInFinalizedDb = true
					parentRootVal := phase0.Root(blockHead.ParentRoot)
					dependentBlock.parentRoot = &parentRootVal
					dependentBlock.forkId = ForkKey(blockHead.ForkId)
					dependentBlock.forkChecked = true
				}
			}
			if dependentBlock != nil && slices.Contains(canonicalForkIds, dependentBlock.forkId) {
				if bestEpochStats == nil || dependentBlock.Slot > bestDistance {
					bestEpochStats = stats
					bestDistance = dependentBlock.Slot
				}
			}
		}

		if bestEpochStats == nil {
			// retry with non ready states
			for _, stats := range epochStats {
				if stats.ready {
					continue
				}

				dependentBlock := indexer.blockCache.getBlockByRoot(stats.dependentRoot)
				if dependentBlock != nil && slices.Contains(canonicalForkIds, dependentBlock.forkId) {
					if bestEpochStats == nil || dependentBlock.Slot > bestDistance {
						bestEpochStats = stats
						bestDistance = dependentBlock.Slot
					}
				}
			}
		}
	}

	if bestEpochStats == nil {
		bestEpochStats = epochStats[0]
	}

	return bestEpochStats
}

// GetParentForkIds returns the parent fork ids of the given fork.
func (indexer *Indexer) GetParentForkIds(forkId ForkKey) []ForkKey {
	return indexer.forkCache.getParentForkIds(forkId)
}

// GetCachedValidatorSetForRoot returns the cached validator set for a given blockRoot.
func (indexer *Indexer) GetCachedValidatorSetForRoot(blockRoot phase0.Root) []ValidatorWithIndex {
	return indexer.validatorCache.getCachedValidatorSetForRoot(blockRoot)
}

// GetCachedValidatorSetForRoot returns the cached validator set for a given blockRoot.
func (indexer *Indexer) GetActiveValidatorDataForRoot(epoch *phase0.Epoch, blockRoot phase0.Root) []ValidatorDataWithIndex {
	return indexer.validatorCache.getActiveValidatorDataForRoot(epoch, blockRoot)
}

// GetValidatorSetSize returns the size of the validator set cache.
func (indexer *Indexer) GetValidatorSetSize() uint64 {
	return indexer.validatorCache.getValidatorSetSize()
}

// GetValidatorFlags returns the validator flags for a given validator index.
func (indexer *Indexer) GetValidatorFlags(validatorIndex phase0.ValidatorIndex) uint16 {
	return indexer.validatorCache.getValidatorFlags(validatorIndex)
}

// GetActivationExitQueueLengths returns the activation and exit queue lengths for the given epoch.
func (indexer *Indexer) GetActivationExitQueueLengths(epoch phase0.Epoch, overrideForkId *ForkKey) (uint64, uint64) {
	canonicalHead := indexer.GetCanonicalHead(overrideForkId)
	if canonicalHead == nil {
		return 0, 0
	}

	return indexer.validatorCache.getActivationExitQueueLengths(epoch, canonicalHead.Root)
}

// GetValidatorIndexByPubkey returns the validator index for a given pubkey.
func (indexer *Indexer) GetValidatorIndexByPubkey(pubkey phase0.BLSPubKey) (phase0.ValidatorIndex, bool) {
	return indexer.pubkeyCache.Get(pubkey)
}

// GetValidatorByIndex returns the validator by index for a given forkId.
func (indexer *Indexer) GetValidatorByIndex(index phase0.ValidatorIndex, overrideForkId *ForkKey) *phase0.Validator {
	return indexer.validatorCache.getValidatorByIndex(index, overrideForkId)
}

// GetValidatorActivity returns the validator activity for a given validator index.
func (indexer *Indexer) GetValidatorActivity(validatorIndex phase0.ValidatorIndex) ([]ValidatorActivity, phase0.Epoch) {
	activity := indexer.validatorActivity.getValidatorActivity(validatorIndex)
	return activity, indexer.validatorActivity.oldestActivityEpoch
}

// GetRecentValidatorBalances returns the most recent validator balances for the given fork.
func (indexer *Indexer) GetRecentValidatorBalances(overrideForkId *ForkKey) []phase0.Gwei {
	chainState := indexer.consensusPool.GetChainState()

	canonicalHead := indexer.GetCanonicalHead(overrideForkId)
	if canonicalHead == nil {
		return nil
	}

	headEpoch := chainState.EpochOfSlot(canonicalHead.Slot)

	var epochStats *EpochStats
	for {
		cEpoch := chainState.EpochOfSlot(canonicalHead.Slot)
		if headEpoch-cEpoch > 2 {
			return nil
		}

		dependentBlock := indexer.blockCache.getDependentBlock(chainState, canonicalHead, nil)
		if dependentBlock == nil {
			return nil
		}
		canonicalHead = dependentBlock

		stats := indexer.epochCache.getEpochStats(cEpoch, dependentBlock.Root)
		if cEpoch > 0 && (stats == nil || stats.dependentState == nil || stats.dependentState.loadingStatus != 2) {
			continue // retry previous state
		}

		epochStats = stats
		break
	}

	if epochStats == nil || epochStats.dependentState == nil {
		return nil
	}

	return epochStats.dependentState.validatorBalances
}

// GetFullValidatorByIndex returns the full validator set entry for a given validator index, including balances and validator status.
// If an overrideForkId is provided, the validator for the fork is returned.
func (indexer *Indexer) GetFullValidatorByIndex(validatorIndex phase0.ValidatorIndex, epoch phase0.Epoch, overrideForkId *ForkKey, withBalances bool) *v1.Validator {
	var epochStats *EpochStats

	if withBalances {
		chainState := indexer.consensusPool.GetChainState()

		canonicalHead := indexer.GetCanonicalHead(overrideForkId)
		if canonicalHead == nil {
			return nil
		}

		headEpoch := chainState.EpochOfSlot(canonicalHead.Slot)

		for {
			cEpoch := chainState.EpochOfSlot(canonicalHead.Slot)
			if headEpoch-cEpoch > 2 {
				return nil
			}

			dependentBlock := indexer.blockCache.getDependentBlock(chainState, canonicalHead, nil)
			if dependentBlock == nil {
				return nil
			}
			canonicalHead = dependentBlock

			stats := indexer.epochCache.getEpochStats(cEpoch, dependentBlock.Root)
			if cEpoch > 0 && (stats == nil || stats.dependentState == nil || stats.dependentState.loadingStatus != 2) {
				continue // retry previous state
			}

			epochStats = stats

			if cEpoch > 0 && stats.epoch > epoch {
				continue
			}

			break
		}
	}

	hasBalances := epochStats != nil && epochStats.dependentState != nil && epochStats.dependentState.loadingStatus == 2

	var basicValidator *phase0.Validator
	if hasBalances {
		basicValidator = indexer.validatorCache.getValidatorByIndexAndRoot(validatorIndex, epochStats.dependentRoot)
	} else {
		basicValidator = indexer.validatorCache.getValidatorByIndex(validatorIndex, overrideForkId)
	}

	if basicValidator == nil {
		return nil
	}

	var balance *phase0.Gwei
	if hasBalances {
		balance = &epochStats.dependentState.validatorBalances[validatorIndex]
	}

	state := v1.ValidatorToState(basicValidator, balance, epoch, FarFutureEpoch)

	validatorData := &v1.Validator{
		Index:     validatorIndex,
		Status:    state,
		Validator: basicValidator,
	}

	if balance != nil {
		validatorData.Balance = *balance
	}

	return validatorData
}
