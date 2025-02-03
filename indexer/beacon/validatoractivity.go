package beacon

import (
	"math"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/attestantio/go-eth2-client/spec/phase0"
)

// validatorActivityCache is the cache for the validator activity.
type validatorActivityCache struct {
	indexer             *Indexer
	activityMap         map[phase0.ValidatorIndex][]ValidatorActivity
	activityMutex       sync.RWMutex // mutex to protect recentActivity for concurrent access
	oldestActivityEpoch phase0.Epoch // oldest epoch in activity cache
}

// ValidatorActivity represents a validator's activity in an epoch.
// entry size: 18 bytes (10 bytes data + 8 bytes pointer)
// max. entries per validator: 3-8 (inMemoryEpochs)
// total memory consumption:
//   - 10k active validators:
//     min: 10000 * 18 * 3 = 540kB = 0.54MB
//     max: 10000 * 18 * 8 = 1440kB = 1.44MB
//   - 100k active validators:
//     min: 100000 * 18 * 3 = 5400kB = 5.4MB
//     max: 100000 * 18 * 8 = 14400kB = 14.4MB
//   - 1M active validators:
//     min: 1000000 * 18 * 3 = 54000kB = 54MB
//     max: 1000000 * 18 * 8 = 144000kB = 144MB
type ValidatorActivity struct {
	VoteBlock *Block // the block where the vote was included
	VoteDelay uint16 // the inclusion delay of the vote in slots
}

// newValidatorActivityCache creates & returns a new instance of validatorActivityCache.
func newValidatorActivityCache(indexer *Indexer) *validatorActivityCache {
	cache := &validatorActivityCache{
		indexer:             indexer,
		activityMap:         make(map[phase0.ValidatorIndex][]ValidatorActivity),
		oldestActivityEpoch: math.MaxInt64,
	}

	go cache.cleanupLoop()

	return cache
}

func (cache *validatorActivityCache) getCutOffEpoch() phase0.Epoch {
	chainState := cache.indexer.consensusPool.GetChainState()
	currentEpoch := chainState.CurrentEpoch()
	cutOffEpoch := phase0.Epoch(0)
	if currentEpoch > phase0.Epoch(cache.indexer.activityHistoryLength) {
		cutOffEpoch = currentEpoch - phase0.Epoch(cache.indexer.activityHistoryLength)
	}

	return cutOffEpoch
}

// updateValidatorActivity updates the validator activity cache.
func (cache *validatorActivityCache) updateValidatorActivity(validatorIndex phase0.ValidatorIndex, epoch phase0.Epoch, dutySlot phase0.Slot, voteBlock *Block) {
	cutOffEpoch := cache.getCutOffEpoch()
	chainState := cache.indexer.consensusPool.GetChainState()

	if epoch < cutOffEpoch {
		// ignore old activity
		return
	}
	if epoch < cache.oldestActivityEpoch {
		cache.oldestActivityEpoch = epoch
	} else if cache.oldestActivityEpoch < cutOffEpoch+1 {
		cache.oldestActivityEpoch = cutOffEpoch + 1
	}

	cache.activityMutex.Lock()
	defer cache.activityMutex.Unlock()

	recentActivity := cache.activityMap[validatorIndex]
	if recentActivity == nil {
		recentActivity = make([]ValidatorActivity, 0, cache.indexer.activityHistoryLength)
	}

	replaceIndex := -1
	cutOffLength := 0
	activityLength := len(recentActivity)
	for i := activityLength - 1; i >= 0; i-- {
		activity := recentActivity[i]
		if activity.VoteBlock == voteBlock {
			// already exists
			return
		}

		dutySlot := phase0.Slot(0)
		if activity.VoteBlock != nil {
			dutySlot = activity.VoteBlock.Slot
		}

		if chainState.EpochOfSlot(dutySlot) < cutOffEpoch {
			recentActivity[i].VoteBlock = nil // clear for gc
			if replaceIndex == -1 {
				replaceIndex = i
			} else if replaceIndex == activityLength-cutOffLength-1 {
				cutOffLength++
				replaceIndex = i
			} else {
				// copy last element to current index
				cutOffLength++
				recentActivity[i] = recentActivity[activityLength-cutOffLength-1]
			}
		}
	}

	if replaceIndex != -1 {
		recentActivity[replaceIndex] = ValidatorActivity{
			VoteBlock: voteBlock,
			VoteDelay: uint16(voteBlock.Slot - dutySlot),
		}

		if cutOffLength > 0 {
			recentActivity = recentActivity[:activityLength-cutOffLength]
		}
	} else {
		recentActivity = append(recentActivity, ValidatorActivity{
			VoteBlock: voteBlock,
			VoteDelay: uint16(voteBlock.Slot - dutySlot),
		})
	}

	cache.activityMap[validatorIndex] = recentActivity
}

// getValidatorActivity returns the validator activity for a given validator index.
func (cache *validatorActivityCache) getValidatorActivity(validatorIndex phase0.ValidatorIndex) []ValidatorActivity {
	cache.activityMutex.RLock()
	defer cache.activityMutex.RUnlock()

	cachedActivity := cache.activityMap[validatorIndex]
	recentActivity := make([]ValidatorActivity, 0, len(cachedActivity))
	for _, activity := range cachedActivity {
		if activity.VoteBlock != nil {
			recentActivity = append(recentActivity, activity)
		}
	}

	sort.Slice(recentActivity, func(i, j int) bool {
		return recentActivity[i].VoteBlock.Slot > recentActivity[j].VoteBlock.Slot
	})

	return recentActivity
}

func (cache *validatorActivityCache) cleanupLoop() {
	defer func() {
		if err := recover(); err != nil {
			cache.indexer.logger.WithError(err.(error)).Errorf("uncaught panic in indexer.beacon.validatorActivityCache.cleanupLoop subroutine: %v, stack: %v", err, string(debug.Stack()))
			time.Sleep(10 * time.Second)

			go cache.cleanupLoop()
		}
	}()

	for {
		time.Sleep(30 * time.Minute)
		cache.cleanupCache()
	}
}

func (cache *validatorActivityCache) cleanupCache() {
	chainState := cache.indexer.consensusPool.GetChainState()
	currentEpoch := chainState.CurrentEpoch()
	cutOffEpoch := phase0.Epoch(0)
	if currentEpoch > phase0.Epoch(cache.indexer.activityHistoryLength) {
		cutOffEpoch = currentEpoch - phase0.Epoch(cache.indexer.activityHistoryLength)
	}

	cache.activityMutex.Lock()
	defer cache.activityMutex.Unlock()

	deleted := 0
	for validatorIndex, recentActivity := range cache.activityMap {
		cutOffLength := 0
		activityLength := len(recentActivity)
		for i := activityLength - 1; i >= 0; i-- {
			activity := recentActivity[i]

			dutySlot := phase0.Slot(0)
			if activity.VoteBlock != nil {
				dutySlot = activity.VoteBlock.Slot
			}

			if chainState.EpochOfSlot(dutySlot) < cutOffEpoch {
				recentActivity[i].VoteBlock = nil // clear for gc
				deleted++

				// copy last element to current index
				cutOffLength++
				if i != activityLength-cutOffLength-1 {
					recentActivity[i] = recentActivity[activityLength-cutOffLength-1]
				}
			}
		}

		if cutOffLength == activityLength {
			delete(cache.activityMap, validatorIndex)
		} else if cutOffLength > 0 {
			cache.activityMap[validatorIndex] = recentActivity[:activityLength-cutOffLength]
		}
	}

	cache.indexer.logger.Infof("cleaned up activity cache, deleted %d entries", deleted)
}
