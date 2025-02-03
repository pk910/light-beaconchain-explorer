package beacon

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/ethpandaops/dora/clients/consensus"
	"github.com/prysmaticlabs/go-bitfield"
)

// epochVotesKey is the primary key for EpochVotes entries in cache.
// consists of dependendRoot (32 byte), epoch (8 byte), highestRoot (32 byte) and blockCount/hasValues (1 byte).
type epochVotesKey [32 + 8 + 32 + 1]byte

// generate epochStatsKey from epoch and dependentRoot
func getEpochVotesKey(epoch phase0.Epoch, dependentRoot phase0.Root, highestRoot phase0.Root, blockCount uint8, hasValues bool, isPrecalc bool) epochVotesKey {
	var key epochVotesKey

	copy(key[0:], dependentRoot[:])
	binary.LittleEndian.PutUint64(key[32:], uint64(epoch))
	copy(key[40:], highestRoot[:])
	key[72] = blockCount
	if hasValues {
		key[72] |= 0x80
	}
	if isPrecalc {
		key[72] |= 0x40
	}

	return key
}

// EpochVotes represents the aggregated votes for an epoch.
type EpochVotes struct {
	CurrentEpoch struct {
		TargetVoteAmount phase0.Gwei
		HeadVoteAmount   phase0.Gwei
		TotalVoteAmount  phase0.Gwei
	}
	NextEpoch struct {
		TargetVoteAmount phase0.Gwei
		HeadVoteAmount   phase0.Gwei
		TotalVoteAmount  phase0.Gwei
	}
	TargetVotePercent float64
	HeadVotePercent   float64
	TotalVotePercent  float64
	AmountIsCount     bool
}

// aggregateEpochVotes aggregates the votes for an epoch based on the provided chain state, blocks, and epoch stats.
func (indexer *Indexer) aggregateEpochVotes(epoch phase0.Epoch, chainState *consensus.ChainState, blocks []*Block, epochStats *EpochStats) *EpochVotes {
	if len(blocks) == 0 {
		return &EpochVotes{}
	}

	var targetRoot phase0.Root
	if chainState.SlotToSlotIndex(blocks[0].Slot) == 0 {
		targetRoot = blocks[0].Root
	} else if parentRoot := blocks[0].GetParentRoot(); parentRoot != nil {
		targetRoot = *parentRoot
	}

	votesWithValues := epochStats != nil && epochStats.ready
	votesWithPrecalc := epochStats != nil && epochStats.precalcValues != nil

	votesKey := getEpochVotesKey(epoch, targetRoot, blocks[len(blocks)-1].Root, uint8(len(blocks)), votesWithValues, votesWithPrecalc)
	if cachedVotes, isOk := indexer.epochCache.votesCache.Get(votesKey); isOk {
		indexer.epochCache.votesCacheHit++
		return cachedVotes
	}

	votes := indexer.aggregateEpochVotesAndActivity(epoch, chainState, blocks, epochStats)
	indexer.epochCache.votesCacheMiss++

	return votes
}

func (indexer *Indexer) aggregateEpochVotesAndActivity(epoch phase0.Epoch, chainState *consensus.ChainState, blocks []*Block, epochStats *EpochStats) *EpochVotes {
	t1 := time.Now()

	var targetRoot phase0.Root
	if chainState.SlotToSlotIndex(blocks[0].Slot) == 0 {
		targetRoot = blocks[0].Root
	} else if parentRoot := blocks[0].GetParentRoot(); parentRoot != nil {
		targetRoot = *parentRoot
	}

	var epochStatsValues *EpochStatsValues
	votesWithPrecalc := false
	votesWithValues := false
	if epochStats != nil {
		epochStatsValues = epochStats.GetOrLoadValues(indexer, true, false)
		votesWithPrecalc = epochStats.precalcValues != nil
		votesWithValues = epochStats.ready
	}
	specs := chainState.GetSpecs()

	votes := &EpochVotes{
		AmountIsCount: epochStatsValues == nil,
	}

	var activityBitlist bitfield.Bitlist

	if epochStatsValues != nil {
		activityBitlist = bitfield.NewBitlist(epochStatsValues.ActiveValidators)
	}

	deduplicationMap := map[voteDeduplicationKey]bool{}

	for _, block := range blocks {
		blockBody := block.GetBlock()
		if blockBody == nil {
			continue
		}

		slot := block.Slot

		isNextEpoch := chainState.EpochOfSlot(slot) > epoch
		attestations, err := blockBody.Attestations()
		if err != nil {
			continue
		}

		processedFlag := uint8(1)
		if isNextEpoch {
			processedFlag = 2
		}
		processActivity := (block.processedActivity&processedFlag == 0) && votesWithValues && !votesWithPrecalc
		if processActivity {
			block.processedActivity |= processedFlag
		}

		for attIdx, attVersioned := range attestations {
			attData, err := attVersioned.Data()
			if err != nil {
				indexer.logger.Debugf("aggregateEpochVotes slot %v failed, can't get data for attestation %v: %v", slot, attIdx, err)
				continue
			}
			if chainState.EpochOfSlot(attData.Slot) != epoch {
				continue
			}

			attAggregationBits, err := attVersioned.AggregationBits()
			if err != nil {
				indexer.logger.Debugf("aggregateEpochVotes slot %v failed, can't get grregation bits for attestation %v: %v", slot, attIdx, err)
				continue
			}

			voteAmount := phase0.Gwei(0)
			slotIndex := chainState.SlotToSlotIndex(attData.Slot)
			updateActivity := func(validatorIndex phase0.ValidatorIndex) {
				if processActivity {
					indexer.validatorActivity.updateValidatorActivity(validatorIndex, epoch, attData.Slot, block)
				}
			}

			if attVersioned.Version >= spec.DataVersionElectra {
				// EIP-7549 changes the attestation aggregation
				// there can now be attestations from all committees aggregated into a single attestation aggregate
				committeeBits, err := attVersioned.CommitteeBits()
				if err != nil {
					indexer.logger.Debugf("aggregateEpochVotes slot %v failed, can't get committeeBits for attestation %v: %v", slot, attIdx, err)
					continue
				}

				aggregationBitsOffset := uint64(0)
				aggregationBitsIndex := uint64(0)

				for _, committee := range committeeBits.BitIndices() {
					if uint64(committee) >= specs.MaxCommitteesPerSlot {
						continue
					}

					if epochStatsValues != nil {
						voteAmt, committeeSize := votes.aggregateVotes(epochStatsValues, slotIndex, uint64(committee), attAggregationBits, aggregationBitsOffset, &activityBitlist, updateActivity)
						voteAmount += voteAmt
						aggregationBitsOffset += committeeSize
					} else {
						voteAmt := votes.aggregateVotesWithoutDuties(deduplicationMap, slotIndex, uint64(committee), attAggregationBits, committeeBits.Count(), aggregationBitsIndex)
						aggregationBitsIndex++
						voteAmount += voteAmt
					}
				}
			} else {
				// pre electra attestation aggregation
				if epochStatsValues != nil {
					voteAmt, _ := votes.aggregateVotes(epochStatsValues, slotIndex, uint64(attData.Index), attAggregationBits, 0, &activityBitlist, updateActivity)
					voteAmount += voteAmt
				} else {
					voteAmt := votes.aggregateVotesWithoutDuties(deduplicationMap, slotIndex, uint64(attData.Index), attAggregationBits, 1, 0)
					voteAmount += voteAmt
				}
			}

			if bytes.Equal(attData.Target.Root[:], targetRoot[:]) {
				if isNextEpoch {
					votes.NextEpoch.TargetVoteAmount += voteAmount
				} else {
					votes.CurrentEpoch.TargetVoteAmount += voteAmount
				}
			} /*else {
				indexer.logger.Infof("vote target missmatch %v != 0x%x", attData.Target.Root, targetRoot)
			}*/
			parentRoot := block.GetParentRoot()

			if parentRoot != nil && bytes.Equal(attData.BeaconBlockRoot[:], parentRoot[:]) {
				if isNextEpoch {
					votes.NextEpoch.HeadVoteAmount += voteAmount
				} else {
					votes.CurrentEpoch.HeadVoteAmount += voteAmount
				}
			}
			if isNextEpoch {
				votes.NextEpoch.TotalVoteAmount += voteAmount
			} else {
				votes.CurrentEpoch.TotalVoteAmount += voteAmount
			}
		}
	}

	if epochStatsValues != nil {
		votes.TargetVotePercent = float64(votes.CurrentEpoch.TargetVoteAmount+votes.NextEpoch.TargetVoteAmount) * 100 / float64(epochStatsValues.EffectiveBalance)
		votes.HeadVotePercent = float64(votes.CurrentEpoch.HeadVoteAmount+votes.NextEpoch.HeadVoteAmount) * 100 / float64(epochStatsValues.EffectiveBalance)
		votes.TotalVotePercent = float64(votes.CurrentEpoch.TotalVoteAmount+votes.NextEpoch.TotalVoteAmount) * 100 / float64(epochStatsValues.EffectiveBalance)
	}

	votesKey := getEpochVotesKey(epoch, targetRoot, blocks[len(blocks)-1].Root, uint8(len(blocks)), votesWithValues, votesWithPrecalc)

	indexer.logger.Debugf("aggregated epoch %v votes in %v (blocks: %v) [0x%x]", epoch, time.Since(t1), len(blocks), votesKey[:])
	indexer.epochCache.votesCache.Add(votesKey, votes)

	return votes
}

// aggregateVotes aggregates the votes for a specific slot and committee based on the provided epoch statistics, aggregation bits, and offset.
func (votes *EpochVotes) aggregateVotes(epochStatsValues *EpochStatsValues, slotIndex phase0.Slot, committee uint64, aggregationBits bitfield.Bitfield, aggregationBitsOffset uint64, activityBitlist *bitfield.Bitlist, updateActivity func(validatorIndex phase0.ValidatorIndex)) (phase0.Gwei, uint64) {
	voteAmount := phase0.Gwei(0)

	voteDuties := epochStatsValues.AttesterDuties[slotIndex][committee]
	for bitIdx, validatorIndice := range voteDuties {
		if aggregationBits.BitAt(uint64(bitIdx) + aggregationBitsOffset) {

			if activityBitlist.BitAt(uint64(validatorIndice)) {
				continue
			}

			effectiveBalance := epochStatsValues.EffectiveBalances[validatorIndice]
			voteAmount += phase0.Gwei(effectiveBalance) * EtherGweiFactor

			activityBitlist.SetBitAt(uint64(validatorIndice), true)

			validatorIndex := epochStatsValues.ActiveIndices[validatorIndice]
			updateActivity(validatorIndex)
		}
	}
	return voteAmount, uint64(len(voteDuties))
}

type voteDeduplicationKey [14]byte // slotIndex (8) + committee (2) + index (4) = 14 bytes

func getVoteDeduplicationKey(slotIndex phase0.Slot, committee uint16, index uint32) voteDeduplicationKey {
	key := voteDeduplicationKey{}
	binary.LittleEndian.PutUint64(key[:], uint64(slotIndex))
	binary.LittleEndian.PutUint16(key[8:], committee)
	binary.LittleEndian.PutUint32(key[10:], index)
	return key
}

func (votes *EpochVotes) aggregateVotesWithoutDuties(deduplicationMap map[voteDeduplicationKey]bool, slotIndex phase0.Slot, committee uint64, aggregationBits bitfield.Bitfield, splitAggregationBits uint64, splitAggregationIndex uint64) phase0.Gwei {
	voteAmount := phase0.Gwei(0)

	bitsLen := aggregationBits.Len() / splitAggregationBits
	aggregationBitsOffset := splitAggregationIndex * bitsLen
	if splitAggregationBits == splitAggregationIndex+1 {
		bitsLen = aggregationBits.Len() - (splitAggregationIndex * splitAggregationBits)
	}

	for bitIdx := uint64(0); bitIdx < bitsLen; bitIdx++ {
		if aggregationBits.BitAt(bitIdx + aggregationBitsOffset) {
			voteKey := getVoteDeduplicationKey(slotIndex, uint16(committee), uint32(bitIdx))
			if deduplicationMap[voteKey] {
				continue
			}

			voteAmount++
			deduplicationMap[voteKey] = true
		}
	}
	return voteAmount
}
