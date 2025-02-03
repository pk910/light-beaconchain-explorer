package beacon

import (
	"reflect"

	mapsize "github.com/520MianXiangDuiXiang520/MapSize"
	"github.com/attestantio/go-eth2-client/spec/phase0"
)

type CacheDebugStats struct {
	BlockCache struct {
		SlotMap      CacheDebugMapSize
		RootMap      CacheDebugMapSize
		ParentMap    CacheDebugMapSize
		ExecBlockMap CacheDebugMapSize
		BlockHeader  uint64
		BlockBodies  uint64
		BlockIndexes uint64
		BlockSize    uint64
	}
	EpochCache struct {
		StatsMap       CacheDebugMapSize
		StateMap       CacheDebugMapSize
		StatsFull      uint64
		StatsPrecalc   uint64
		StatsPruned    uint64
		StateLoaded    uint64
		VotesCacheLen  uint64
		VotesCacheHit  uint64
		VotesCacheMiss uint64
	}
	ForkCache struct {
		ForkMap            CacheDebugMapSize
		ParentIdCacheLen   uint64
		ParentIdCacheHit   uint64
		ParentIdCacheMiss  uint64
		ParentIdsCacheLen  uint64
		ParentIdsCacheHit  uint64
		ParentIdsCacheMiss uint64
	}
	ValidatorCache struct {
		Validators        uint64
		ValidatorDiffs    uint64
		ValidatorData     uint64
		ValidatorActivity uint64
		PubkeyMap         CacheDebugMapSize
	}
}

type CacheDebugMapSize struct {
	Length int
	Size   int64
}

func (indexer *Indexer) GetCacheDebugStats() *CacheDebugStats {
	cacheStats := &CacheDebugStats{}
	indexer.getBlockCacheDebugStats(cacheStats)
	indexer.getEpochCacheDebugStats(cacheStats)
	indexer.getForkCacheDebugStats(cacheStats)
	indexer.getValidatorCacheDebugStats(cacheStats)
	return cacheStats
}

func (indexer *Indexer) getBlockCacheDebugStats(cacheStats *CacheDebugStats) {
	indexer.blockCache.cacheMutex.RLock()
	defer indexer.blockCache.cacheMutex.RUnlock()

	cacheStats.BlockCache.SlotMap = CacheDebugMapSize{
		Length: len(indexer.blockCache.slotMap),
		Size:   mapsize.Size(indexer.blockCache.slotMap),
	}
	cacheStats.BlockCache.RootMap = CacheDebugMapSize{
		Length: len(indexer.blockCache.rootMap),
		Size:   mapsize.Size(indexer.blockCache.rootMap),
	}
	cacheStats.BlockCache.ParentMap = CacheDebugMapSize{
		Length: len(indexer.blockCache.parentMap),
		Size:   mapsize.Size(indexer.blockCache.parentMap),
	}
	cacheStats.BlockCache.ExecBlockMap = CacheDebugMapSize{
		Length: len(indexer.blockCache.execBlockMap),
		Size:   mapsize.Size(indexer.blockCache.execBlockMap),
	}

	for _, block := range indexer.blockCache.rootMap {
		if block.header != nil {
			cacheStats.BlockCache.BlockHeader++
		}
		if block.block != nil {
			cacheStats.BlockCache.BlockBodies++
		}
		if block.blockIndex != nil {
			cacheStats.BlockCache.BlockIndexes++
		}
	}

	cacheStats.BlockCache.BlockSize = uint64(reflect.TypeOf(Block{}).Size())
}

func (indexer *Indexer) getEpochCacheDebugStats(cacheStats *CacheDebugStats) {
	indexer.epochCache.cacheMutex.RLock()
	defer indexer.epochCache.cacheMutex.RUnlock()

	cacheStats.EpochCache.StatsMap = CacheDebugMapSize{
		Length: len(indexer.epochCache.statsMap),
		Size:   mapsize.Size(indexer.epochCache.statsMap),
	}
	cacheStats.EpochCache.StateMap = CacheDebugMapSize{
		Length: len(indexer.epochCache.stateMap),
		Size:   mapsize.Size(indexer.epochCache.stateMap),
	}

	for _, stats := range indexer.epochCache.statsMap {
		if stats.values != nil {
			cacheStats.EpochCache.StatsFull++
		}
		if stats.precalcValues != nil {
			cacheStats.EpochCache.StatsPrecalc++
		}
		if stats.prunedValues != nil {
			cacheStats.EpochCache.StatsPruned++
		}
	}

	for _, state := range indexer.epochCache.stateMap {
		if state.loadingStatus == 2 {
			cacheStats.EpochCache.StateLoaded++
		}
	}

	cacheStats.EpochCache.VotesCacheLen = uint64(indexer.epochCache.votesCache.Len())
	cacheStats.EpochCache.VotesCacheHit = indexer.epochCache.votesCacheHit
	cacheStats.EpochCache.VotesCacheMiss = indexer.epochCache.votesCacheMiss
}

func (indexer *Indexer) getForkCacheDebugStats(cacheStats *CacheDebugStats) {
	indexer.forkCache.cacheMutex.RLock()
	defer indexer.forkCache.cacheMutex.RUnlock()

	cacheStats.ForkCache.ForkMap = CacheDebugMapSize{
		Length: len(indexer.forkCache.forkMap),
		Size:   mapsize.Size(indexer.forkCache.forkMap),
	}

	cacheStats.ForkCache.ParentIdCacheLen = uint64(indexer.forkCache.parentIdCache.Len())
	cacheStats.ForkCache.ParentIdCacheHit = indexer.forkCache.parentIdCacheHit
	cacheStats.ForkCache.ParentIdCacheMiss = indexer.forkCache.parentIdCacheMiss

	cacheStats.ForkCache.ParentIdsCacheLen = uint64(indexer.forkCache.parentIdsCache.Len())
	cacheStats.ForkCache.ParentIdsCacheHit = indexer.forkCache.parentIdsCacheHit
	cacheStats.ForkCache.ParentIdsCacheMiss = indexer.forkCache.parentIdsCacheMiss
}

func (indexer *Indexer) getValidatorCacheDebugStats(cacheStats *CacheDebugStats) {
	indexer.validatorCache.cacheMutex.RLock()
	defer indexer.validatorCache.cacheMutex.RUnlock()

	cacheStats.ValidatorCache.Validators = uint64(len(indexer.validatorCache.valsetCache))

	validatorsMap := map[*phase0.Validator]bool{}
	for _, validator := range indexer.validatorCache.valsetCache {
		refs := len(validator.validatorDiffs)
		for _, diff := range validator.validatorDiffs {
			validatorsMap[diff.validator] = true
		}

		if validator.finalValidator != nil {
			validatorsMap[validator.finalValidator] = true
			refs++
		}
		cacheStats.ValidatorCache.ValidatorDiffs += uint64(refs)
	}

	cacheStats.ValidatorCache.ValidatorData = uint64(len(validatorsMap))

	for _, recentActivity := range indexer.validatorActivity.activityMap {
		cacheStats.ValidatorCache.ValidatorActivity += uint64(len(recentActivity))
	}

	if indexer.pubkeyCache.pubkeyMap != nil {
		cacheStats.ValidatorCache.PubkeyMap = CacheDebugMapSize{
			Length: len(indexer.pubkeyCache.pubkeyMap),
			Size:   mapsize.Size(indexer.pubkeyCache.pubkeyMap),
		}
	}
}
