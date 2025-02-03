package services

import (
	"bytes"
	"slices"
	"sort"
	"strings"

	v1 "github.com/attestantio/go-eth2-client/api/v1"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/ethpandaops/dora/db"
	"github.com/ethpandaops/dora/dbtypes"
	"github.com/ethpandaops/dora/indexer/beacon"
)

func (bs *ChainService) GetValidatorByIndex(index phase0.ValidatorIndex, withBalance bool) *v1.Validator {
	currentEpoch := bs.consensusPool.GetChainState().CurrentEpoch()
	return bs.beaconIndexer.GetFullValidatorByIndex(index, currentEpoch, nil, withBalance)
}

func (bs *ChainService) GetValidatorIndexByPubkey(pubkey phase0.BLSPubKey) (phase0.ValidatorIndex, bool) {
	return bs.beaconIndexer.GetValidatorIndexByPubkey(pubkey)
}

func (bs *ChainService) GetActiveValidatorData() []beacon.ValidatorDataWithIndex {
	canonicalHead := bs.beaconIndexer.GetCanonicalHead(nil)
	if canonicalHead == nil {
		return nil
	}

	return bs.beaconIndexer.GetActiveValidatorDataForRoot(nil, canonicalHead.Root)
}

// getValidatorsByWithdrawalAddressForRoot returns validators with a specific withdrawal address for a given blockRoot
func (bs *ChainService) GetFilteredValidatorSet(filter *dbtypes.ValidatorFilter, withBalance bool) ([]v1.Validator, uint64) {
	var overrideForkId *beacon.ForkKey

	canonicalHead := bs.beaconIndexer.GetCanonicalHead(overrideForkId)
	if canonicalHead == nil {
		return nil, 0
	}

	var balances []phase0.Gwei
	if withBalance {
		balances = bs.beaconIndexer.GetRecentValidatorBalances(overrideForkId)
	}
	currentEpoch := bs.consensusPool.GetChainState().CurrentEpoch()

	cachedResults := make([]beacon.ValidatorWithIndex, 0, 1000)
	cachedIndexes := map[uint64]bool{}

	// get matching entries from cached validators
	for _, cachedValidator := range bs.beaconIndexer.GetCachedValidatorSetForRoot(canonicalHead.Root) {
		if filter.MinIndex != nil && cachedValidator.Index < *filter.MinIndex {
			continue
		}
		if filter.MaxIndex != nil && cachedValidator.Index > *filter.MaxIndex {
			continue
		}
		if filter.WithdrawalAddress != nil {
			if cachedValidator.Validator.WithdrawalCredentials[0] != 0x01 && cachedValidator.Validator.WithdrawalCredentials[0] != 0x02 {
				continue
			}
			if !bytes.Equal(cachedValidator.Validator.WithdrawalCredentials[12:], filter.WithdrawalAddress[:]) {
				continue
			}
		}
		if filter.ValidatorName != "" {
			vname := bs.validatorNames.GetValidatorName(cachedValidator.Index)
			if !strings.Contains(vname, filter.ValidatorName) {
				continue
			}
		}
		if len(filter.Status) > 0 {
			var balancePtr *phase0.Gwei
			if balances != nil {
				balancePtr = &balances[cachedValidator.Index]
			}
			validatorState := v1.ValidatorToState(cachedValidator.Validator, balancePtr, currentEpoch, beacon.FarFutureEpoch)
			if !slices.Contains(filter.Status, validatorState) {
				continue
			}
		}

		cachedResults = append(cachedResults, cachedValidator)
		cachedIndexes[cachedValidator.Index] = true
	}

	// get matching entries from DB
	dbIndexes, err := db.GetValidatorIndexesByFilter(*filter, uint64(currentEpoch))
	if err != nil {
		bs.logger.Warnf("error getting validator indexes by filter: %v", err)
		return nil, 0
	}

	// sort results
	var sortFn func(valA, valB beacon.ValidatorWithIndex) bool
	switch filter.OrderBy {
	case dbtypes.ValidatorOrderIndexAsc:
		sortFn = func(valA, valB beacon.ValidatorWithIndex) bool {
			return valA.Index < valB.Index
		}
	case dbtypes.ValidatorOrderIndexDesc:
		sortFn = func(valA, valB beacon.ValidatorWithIndex) bool {
			return valA.Index > valB.Index
		}
	case dbtypes.ValidatorOrderPubKeyAsc:
		sortFn = func(valA, valB beacon.ValidatorWithIndex) bool {
			return bytes.Compare(valA.Validator.PublicKey[:], valB.Validator.PublicKey[:]) < 0
		}
	case dbtypes.ValidatorOrderPubKeyDesc:
		sortFn = func(valA, valB beacon.ValidatorWithIndex) bool {
			return bytes.Compare(valA.Validator.PublicKey[:], valB.Validator.PublicKey[:]) > 0
		}
	case dbtypes.ValidatorOrderBalanceAsc:
		if balances == nil {
			sortFn = func(valA, valB beacon.ValidatorWithIndex) bool {
				return valA.Validator.EffectiveBalance < valB.Validator.EffectiveBalance
			}
		} else {
			sortFn = func(valA, valB beacon.ValidatorWithIndex) bool {
				return balances[valA.Index] < balances[valB.Index]
			}
			sort.Slice(dbIndexes, func(i, j int) bool {
				return balances[dbIndexes[i]] < balances[dbIndexes[j]]
			})
		}
	case dbtypes.ValidatorOrderBalanceDesc:
		if balances == nil {
			sortFn = func(valA, valB beacon.ValidatorWithIndex) bool {
				return valA.Validator.EffectiveBalance > valB.Validator.EffectiveBalance
			}
		} else {
			sortFn = func(valA, valB beacon.ValidatorWithIndex) bool {
				return balances[valA.Index] > balances[valB.Index]
			}
			sort.Slice(dbIndexes, func(i, j int) bool {
				return balances[dbIndexes[i]] > balances[dbIndexes[j]]
			})
		}
	case dbtypes.ValidatorOrderActivationEpochAsc:
		sortFn = func(valA, valB beacon.ValidatorWithIndex) bool {
			return valA.Validator.ActivationEpoch < valB.Validator.ActivationEpoch
		}
	case dbtypes.ValidatorOrderActivationEpochDesc:
		sortFn = func(valA, valB beacon.ValidatorWithIndex) bool {
			return valA.Validator.ActivationEpoch > valB.Validator.ActivationEpoch
		}
	case dbtypes.ValidatorOrderExitEpochAsc:
		sortFn = func(valA, valB beacon.ValidatorWithIndex) bool {
			return valA.Validator.ExitEpoch < valB.Validator.ExitEpoch
		}
	case dbtypes.ValidatorOrderExitEpochDesc:
		sortFn = func(valA, valB beacon.ValidatorWithIndex) bool {
			return valA.Validator.ExitEpoch > valB.Validator.ExitEpoch
		}
	case dbtypes.ValidatorOrderWithdrawableEpochAsc:
		sortFn = func(valA, valB beacon.ValidatorWithIndex) bool {
			return valA.Validator.WithdrawableEpoch < valB.Validator.WithdrawableEpoch
		}
	case dbtypes.ValidatorOrderWithdrawableEpochDesc:
		sortFn = func(valA, valB beacon.ValidatorWithIndex) bool {
			return valA.Validator.WithdrawableEpoch > valB.Validator.WithdrawableEpoch
		}
	}

	sort.Slice(cachedResults, func(i, j int) bool {
		return sortFn(cachedResults[i], cachedResults[j])
	})

	// stream validator set from db and merge cached results
	resCap := filter.Limit
	if resCap == 0 {
		resCap = uint64(len(cachedResults) + len(dbIndexes))
	}
	result := make([]v1.Validator, 0, resCap)
	cachedIndex := 0
	matchingCount := uint64(0)
	resultCount := uint64(0)
	dbEntryCount := uint64(0)

	db.StreamValidatorsByIndexes(dbIndexes, func(validator *dbtypes.Validator) bool {
		dbEntryCount++
		validatorWithIndex := beacon.ValidatorWithIndex{
			Index:     validator.ValidatorIndex,
			Validator: beacon.UnwrapDbValidator(validator),
		}

		for cachedIndex < len(cachedResults) && (cachedResults[cachedIndex].Index == validatorWithIndex.Index || sortFn(cachedResults[cachedIndex], validatorWithIndex)) {
			if matchingCount >= filter.Offset {
				balance := phase0.Gwei(0)
				var balancePtr *phase0.Gwei
				if balances != nil {
					balance = balances[cachedResults[cachedIndex].Index]
					balancePtr = &balance
				}
				result = append(result, v1.Validator{
					Index:     phase0.ValidatorIndex(cachedResults[cachedIndex].Index),
					Balance:   balance,
					Status:    v1.ValidatorToState(cachedResults[cachedIndex].Validator, balancePtr, currentEpoch, beacon.FarFutureEpoch),
					Validator: cachedResults[cachedIndex].Validator,
				})
				resultCount++
			}
			matchingCount++
			cachedIndex++

			if filter.Limit > 0 && resultCount >= filter.Limit {
				return false // stop streaming
			}
		}

		if cachedIndexes[validator.ValidatorIndex] {
			return true // skip this index, cache entry is newer
		}

		if matchingCount >= filter.Offset {
			balance := phase0.Gwei(0)
			var balancePtr *phase0.Gwei
			if balances != nil {
				balance = balances[validator.ValidatorIndex]
				balancePtr = &balance
			}
			validatorData := beacon.UnwrapDbValidator(validator)
			result = append(result, v1.Validator{
				Index:     phase0.ValidatorIndex(validator.ValidatorIndex),
				Balance:   balance,
				Status:    v1.ValidatorToState(validatorData, balancePtr, currentEpoch, beacon.FarFutureEpoch),
				Validator: validatorData,
			})
			resultCount++
		}
		matchingCount++

		if filter.Limit > 0 && resultCount >= filter.Limit {
			return false // stop streaming
		}

		return true // get more from db
	})

	for cachedIndex < len(cachedResults) && (filter.Limit == 0 || resultCount < filter.Limit) {
		if matchingCount >= filter.Offset {
			balance := phase0.Gwei(0)
			var balancePtr *phase0.Gwei
			if balances != nil {
				balance = balances[cachedResults[cachedIndex].Index]
				balancePtr = &balance
			}
			result = append(result, v1.Validator{
				Index:     phase0.ValidatorIndex(cachedResults[cachedIndex].Index),
				Balance:   balance,
				Status:    v1.ValidatorToState(cachedResults[cachedIndex].Validator, balancePtr, currentEpoch, beacon.FarFutureEpoch),
				Validator: cachedResults[cachedIndex].Validator,
			})
			resultCount++
		}
		matchingCount++
		cachedIndex++
	}

	// add remaining cached results
	matchingCount += uint64(len(cachedResults) - cachedIndex)

	// add remaining db results
	remainingDbCount := uint64(0)
	for i := dbEntryCount; i < uint64(len(dbIndexes)); i++ {
		if cachedIndexes[dbIndexes[i]] {
			continue
		}
		remainingDbCount++
	}
	matchingCount += remainingDbCount

	return result, matchingCount
}
