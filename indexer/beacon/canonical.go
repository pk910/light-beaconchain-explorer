package beacon

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/attestantio/go-eth2-client/spec/phase0"
)

const FarFutureEpoch = phase0.Epoch(math.MaxUint64)

// ChainHead represents a head block of the chain.
type ChainHead struct {
	HeadBlock             *Block      // The head block of the chain.
	AggregatedHeadVotes   phase0.Gwei // The aggregated votes of the last 2 epochs for the head block.
	PerEpochVotingPercent []float64   // The voting percentage in the last epochs (ascendeing order).
}

// GetCanonicalHead returns the canonical head block of the chain.
func (indexer *Indexer) GetCanonicalHead(overrideForkId *ForkKey) *Block {
	indexer.computeCanonicalChain()

	if overrideForkId != nil && indexer.canonicalHead != nil && indexer.canonicalHead.forkId != *overrideForkId {
		chainHeads := indexer.cachedChainHeads

		for _, chainHead := range chainHeads {
			parentForkIds := indexer.forkCache.getParentForkIds(chainHead.HeadBlock.forkId)
			if !slices.Contains(parentForkIds, *overrideForkId) {
				continue
			}

			return chainHead.HeadBlock
		}
	}

	return indexer.canonicalHead
}

// GetChainHeads returns the chain heads sorted by voting percentages.
func (indexer *Indexer) GetChainHeads() []*ChainHead {
	indexer.computeCanonicalChain()

	heads := make([]*ChainHead, len(indexer.cachedChainHeads))
	copy(heads, indexer.cachedChainHeads)

	return heads
}

func (indexer *Indexer) IsCanonicalBlock(block *Block, overrideForkId *ForkKey) bool {
	canonicalHead := indexer.GetCanonicalHead(overrideForkId)
	return indexer.IsCanonicalBlockByHead(block, canonicalHead)
}

func (indexer *Indexer) IsCanonicalBlockByHead(block *Block, headBlock *Block) bool {
	if headBlock == nil || block == nil {
		return false
	}

	if block.forkChecked && headBlock.forkChecked {
		parentForkIds := indexer.forkCache.getParentForkIds(headBlock.forkId)
		return slices.Contains(parentForkIds, block.forkId)
	}

	return indexer.blockCache.isCanonicalBlock(block.Root, headBlock.Root)
}

// computeCanonicalChain computes the canonical chain and updates the indexer's state.
func (indexer *Indexer) computeCanonicalChain() bool {
	indexer.canonicalHeadMutex.Lock()
	defer indexer.canonicalHeadMutex.Unlock()

	if indexer.blockCache.latestBlock == nil {
		return false
	}

	latestBlockRoot := indexer.blockCache.latestBlock.Root
	if bytes.Equal(latestBlockRoot[:], indexer.canonicalComputation[:]) {
		return false
	}

	var headBlock *Block = nil
	var chainHeads []*ChainHead = nil

	chainState := indexer.consensusPool.GetChainState()
	specs := chainState.GetSpecs()
	aggregateEpochs := (32 / specs.SlotsPerEpoch) + 1 // aggregate votes of last 48 slots (2 epochs for mainnet, 5 epochs for minimal config)
	if aggregateEpochs < 2 {
		aggregateEpochs = 2
	}

	t1 := time.Now()

	defer func() {
		indexer.canonicalHead = headBlock
		indexer.cachedChainHeads = chainHeads
		indexer.canonicalComputation = latestBlockRoot

		if headBlock == nil {
			indexer.logger.Warnf("canonical head computation failed. forks: %v, latest block: %v, time: %v ms", len(chainHeads), latestBlockRoot.String(), time.Since(t1).Milliseconds())
		} else {
			indexer.logger.Infof("canonical head computation complete. forks: %v, head: %v (%v), time: %v ms", len(chainHeads), headBlock.Slot, headBlock.Root.String(), time.Since(t1).Milliseconds())
		}
	}()

	headForks := indexer.forkCache.getForkHeads()

	// compare forks, select the one with the most votes
	headForkVotes := map[ForkKey]phase0.Gwei{}
	chainHeads = make([]*ChainHead, 0, len(headForks))
	var bestForkVotes phase0.Gwei = 0

	for _, fork := range headForks {
		if fork.Block == nil {
			continue
		}

		forkVotes, epochParticipation := indexer.aggregateForkVotes(fork.ForkId, aggregateEpochs)
		headForkVotes[fork.ForkId] = forkVotes
		chainHeads = append(chainHeads, &ChainHead{
			HeadBlock:             fork.Block,
			AggregatedHeadVotes:   forkVotes,
			PerEpochVotingPercent: epochParticipation,
		})

		if forkVotes > 0 {
			participationStr := make([]string, len(epochParticipation))
			for i, p := range epochParticipation {
				participationStr[i] = fmt.Sprintf("%.2f%%", p)
			}

			indexer.logger.Infof(
				"fork %v: votes in last 2 epochs: %v ETH (%v), head: %v (%v)",
				fork.ForkId,
				forkVotes/EtherGweiFactor,
				strings.Join(participationStr, ", "),
				fork.Block.Slot,
				fork.Block.Root.String(),
			)
		}

		if forkVotes > bestForkVotes || headBlock == nil {
			bestForkVotes = forkVotes
			headBlock = fork.Block
		} else if forkVotes == bestForkVotes && headBlock.Slot < fork.Block.Slot {
			headBlock = fork.Block
		}
	}

	if headBlock == nil {
		// just get latest block
		latestBlocks := indexer.blockCache.getLatestBlocks(1, nil)
		if len(latestBlocks) > 0 {
			headBlock = latestBlocks[0]

			forkVotes, epochParticipation := indexer.aggregateForkVotes(headBlock.forkId, aggregateEpochs)
			participationStr := make([]string, len(epochParticipation))
			for i, p := range epochParticipation {
				participationStr[i] = fmt.Sprintf("%.2f%%", p)
			}

			indexer.logger.Infof(
				"fallback fork %v votes in last %v epochs: %v ETH (%v), head: %v (%v)",
				headBlock.forkId,
				aggregateEpochs,
				forkVotes/EtherGweiFactor,
				strings.Join(participationStr, ", "),
				headBlock.Slot,
				headBlock.Root.String(),
			)

			chainHeads = []*ChainHead{{
				HeadBlock:             headBlock,
				AggregatedHeadVotes:   forkVotes,
				PerEpochVotingPercent: epochParticipation,
			}}
		}
	}

	slices.SortFunc(chainHeads, func(headA, headB *ChainHead) int {
		percentagesA := float64(0)
		percentagesB := float64(0)
		for k := range headA.PerEpochVotingPercent {
			factor := float64(1)
			if k == len(headA.PerEpochVotingPercent)-1 {
				factor = 0.5
			}
			percentagesA += headA.PerEpochVotingPercent[k] * factor
			if len(headB.PerEpochVotingPercent) > k {
				percentagesB += headB.PerEpochVotingPercent[k] * factor
			}
		}

		if percentagesA != percentagesB {
			return int((percentagesB - percentagesA) * 100)
		}

		return int(headB.HeadBlock.Slot - headA.HeadBlock.Slot)
	})

	return true
}

// aggregateForkVotes aggregates the votes for a given fork.
func (indexer *Indexer) aggregateForkVotes(forkId ForkKey, epochLimit uint64) (totalVotes phase0.Gwei, epochPercent []float64) {
	chainState := indexer.consensusPool.GetChainState()
	specs := chainState.GetSpecs()
	currentEpoch := chainState.CurrentEpoch()

	epochPercent = make([]float64, 0, epochLimit)
	if epochLimit == 0 {
		return
	}

	minAggregateEpoch := currentEpoch
	if minAggregateEpoch > phase0.Epoch(epochLimit)-1 {
		minAggregateEpoch -= phase0.Epoch(epochLimit) - 1
	} else {
		minAggregateEpoch = 0
	}
	minAggregateSlot := chainState.EpochStartSlot(minAggregateEpoch)

	fork := indexer.forkCache.getForkById(forkId)
	if fork != nil && fork.headBlock != nil && fork.headBlock.Slot < minAggregateSlot {
		// fork head block is outside aggregation range
		// very old fork, skip aggregation and return 0
		return
	}

	// get all blocks for given fork (and its parents) from the last epochs
	lastBlocks := []*Block{}
	lastSlot := phase0.Slot(0)
	thisForkId := forkId
	for {
		for _, block := range indexer.blockCache.getLatestBlocks(epochLimit*specs.SlotsPerEpoch, &thisForkId) {
			lastSlot = block.Slot
			if block.Slot < minAggregateSlot {
				break
			}
			lastBlocks = append(lastBlocks, block)
		}

		if lastSlot < minAggregateSlot {
			break
		}

		fork := indexer.forkCache.getForkById(thisForkId)
		if fork == nil {
			break
		}

		thisForkId = fork.parentFork
	}

	if len(lastBlocks) == 0 {
		return
	}

	// already sorted descending by getLatestBlocks, reverse to ascending for aggregation
	slices.Reverse(lastBlocks)

	// aggregate votes per epoch
	lastBlockIdx := 0
	for epoch := minAggregateEpoch; epoch <= currentEpoch; epoch++ {
		epochVotingBlocks := []*Block{}
		nextBlockIdx := 0
		for lastBlockIdx < len(lastBlocks) {
			if chainState.EpochOfSlot(lastBlocks[lastBlockIdx].Slot) == epoch {
				epochVotingBlocks = append(epochVotingBlocks, lastBlocks[lastBlockIdx])
				lastBlockIdx++
			} else if lastBlockIdx+nextBlockIdx < len(lastBlocks) && chainState.EpochOfSlot(lastBlocks[lastBlockIdx+nextBlockIdx].Slot) == epoch+1 {
				epochVotingBlocks = append(epochVotingBlocks, lastBlocks[lastBlockIdx+nextBlockIdx])
				nextBlockIdx++
			} else {
				break
			}
		}

		if len(epochVotingBlocks) == 0 {
			epochPercent = append(epochPercent, 0)
			continue
		}

		dependentRoot := epochVotingBlocks[0].GetParentRoot()
		if dependentRoot == nil {
			epochPercent = append(epochPercent, 0)
			continue
		}

		epochStats := indexer.epochCache.getEpochStats(epoch, *dependentRoot)
		if epochStats == nil {
			epochPercent = append(epochPercent, 0)
			continue
		}

		epochVotes := indexer.aggregateEpochVotes(epoch, chainState, epochVotingBlocks, epochStats)
		if epochVotes.AmountIsCount {
			totalVotes += (epochVotes.CurrentEpoch.TargetVoteAmount + epochVotes.NextEpoch.TargetVoteAmount) * 32 * EtherGweiFactor
		} else {
			totalVotes += epochVotes.CurrentEpoch.TargetVoteAmount + epochVotes.NextEpoch.TargetVoteAmount
		}

		lastBlock := epochVotingBlocks[len(epochVotingBlocks)-1]
		epochProgress := float64(100)

		if chainState.EpochOfSlot(lastBlock.Slot) == epoch {
			lastBlockIndex := chainState.SlotToSlotIndex(lastBlock.Slot)
			if lastBlockIndex > 0 {
				epochProgress = float64(100*lastBlockIndex) / float64(chainState.GetSpecs().SlotsPerEpoch)
			} else {
				epochProgress = 0
			}
		}

		var participationExtrapolation float64
		if epochProgress == 0 {
			participationExtrapolation = 0
		} else {
			participationExtrapolation = 100 * epochVotes.TargetVotePercent / epochProgress
		}
		if participationExtrapolation > 100 {
			participationExtrapolation = 100
		}

		epochPercent = append(epochPercent, participationExtrapolation)
	}

	return
}
