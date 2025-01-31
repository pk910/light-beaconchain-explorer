package beacon

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethpandaops/dora/db"
	"github.com/ethpandaops/dora/dbtypes"
	"github.com/jmoiron/sqlx"
)

// forkCache is a struct that represents the fork cache in the indexer.
type forkCache struct {
	indexer         *Indexer
	cacheMutex      sync.RWMutex
	forkMap         map[ForkKey]*Fork
	finalizedForkId ForkKey
	lastForkId      ForkKey
	parentIdCache   *lru.Cache[ForkKey, ForkKey]
	parentIdsCache  *lru.Cache[ForkKey, []ForkKey]
	forkProcessLock sync.Mutex
}

// newForkCache creates a new instance of the forkCache struct.
func newForkCache(indexer *Indexer) *forkCache {
	return &forkCache{
		indexer:        indexer,
		forkMap:        make(map[ForkKey]*Fork),
		parentIdCache:  lru.NewCache[ForkKey, ForkKey](1000),
		parentIdsCache: lru.NewCache[ForkKey, []ForkKey](30),
	}
}

// loadForkState loads the fork state from the database.
func (cache *forkCache) loadForkState() error {
	forkState := dbtypes.IndexerForkState{}
	db.GetExplorerState("indexer.forkstate", &forkState)

	if forkState.ForkId == 0 {
		forkState.ForkId = 1
	}
	if forkState.Finalized == 0 {
		forkState.Finalized = 1
	}

	cache.lastForkId = ForkKey(forkState.ForkId)
	cache.finalizedForkId = ForkKey(forkState.Finalized)

	return nil
}

// updateForkState updates the fork state in the database.
func (cache *forkCache) updateForkState(tx *sqlx.Tx) error {
	err := db.SetExplorerState("indexer.forkstate", &dbtypes.IndexerForkState{
		ForkId:    uint64(cache.lastForkId),
		Finalized: uint64(cache.finalizedForkId),
	}, tx)
	if err != nil {
		return fmt.Errorf("error while updating fork state: %v", err)
	}
	return nil
}

// getForkById retrieves a fork from the cache by its ID.
func (cache *forkCache) getForkById(forkId ForkKey) *Fork {
	cache.cacheMutex.RLock()
	defer cache.cacheMutex.RUnlock()

	return cache.forkMap[forkId]
}

// addFork adds a fork to the cache.
func (cache *forkCache) addFork(fork *Fork) {
	cache.cacheMutex.Lock()
	defer cache.cacheMutex.Unlock()

	cache.forkMap[fork.forkId] = fork
}

// getForkByLeaf retrieves a fork from the cache by its leaf root.
func (cache *forkCache) getForkByLeaf(leafRoot phase0.Root) *Fork {
	cache.cacheMutex.Lock()
	defer cache.cacheMutex.Unlock()

	for _, fork := range cache.forkMap {
		if bytes.Equal(fork.leafRoot[:], leafRoot[:]) {
			return fork
		}
	}

	return nil
}

// getForkByBase retrieves forks from the cache by their base root.
func (cache *forkCache) getForkByBase(baseRoot phase0.Root) []*Fork {
	cache.cacheMutex.Lock()
	defer cache.cacheMutex.Unlock()

	forks := []*Fork{}
	for _, fork := range cache.forkMap {
		if bytes.Equal(fork.baseRoot[:], baseRoot[:]) {
			forks = append(forks, fork)
		}
	}

	return forks
}

// removeFork removes a fork from the cache.
func (cache *forkCache) removeFork(forkId ForkKey) {
	cache.cacheMutex.Lock()
	defer cache.cacheMutex.Unlock()

	delete(cache.forkMap, forkId)
}

// getParentForkIds returns the parent fork ids of the given fork.
func (cache *forkCache) getParentForkIds(forkId ForkKey) []ForkKey {
	parentForks, isCached := cache.parentIdsCache.Get(forkId)
	if isCached {
		cache.indexer.metrics.forkCacheParentIdsCacheHit.Inc()
		return parentForks
	}

	parentForks = []ForkKey{forkId}
	parentForkId := forkId

	for parentForkId > 1 {
		if cachedParent, isCached := cache.parentIdCache.Get(parentForkId); isCached {
			cache.indexer.metrics.forkCacheParentIdCacheHit.Inc()
			parentForkId = cachedParent
		} else if parentFork := cache.getForkById(parentForkId); parentFork != nil {
			parentForkId = parentFork.parentFork
		} else if dbFork := db.GetForkById(uint64(parentForkId)); dbFork != nil {
			cache.parentIdCache.Add(ForkKey(parentForkId), ForkKey(dbFork.ParentFork))
			parentForkId = ForkKey(dbFork.ParentFork)
			cache.indexer.metrics.forkCacheParentIdCacheMiss.Inc()
		} else {
			cache.parentIdCache.Add(ForkKey(parentForkId), ForkKey(0))
			parentForkId = 0
			cache.indexer.metrics.forkCacheParentIdCacheMiss.Inc()
		}

		parentForks = append(parentForks, parentForkId)
	}

	cache.parentIdsCache.Add(forkId, parentForks)
	cache.indexer.metrics.forkCacheParentIdsCacheMiss.Inc()

	return parentForks
}

// ForkHead represents a fork head with its ID, fork, and block.
type ForkHead struct {
	ForkId ForkKey
	Fork   *Fork
	Block  *Block
}

// getForkHeads returns the fork heads in the cache.
// A head fork is a fork that no other fork is building on top of, so it contains the head of the fork chain.
func (cache *forkCache) getForkHeads() []*ForkHead {
	cache.cacheMutex.RLock()
	defer cache.cacheMutex.RUnlock()

	forkParents := map[ForkKey]bool{}
	for _, fork := range cache.forkMap {
		if fork.parentFork != 0 {
			forkParents[fork.parentFork] = true
		}
	}

	forkHeads := []*ForkHead{}
	if !forkParents[cache.finalizedForkId] {
		canonicalBlocks := cache.indexer.blockCache.getForkBlocks(cache.finalizedForkId)
		sort.Slice(canonicalBlocks, func(i, j int) bool {
			return canonicalBlocks[i].Slot > canonicalBlocks[j].Slot
		})
		if len(canonicalBlocks) > 0 {
			forkHeads = append(forkHeads, &ForkHead{
				ForkId: cache.finalizedForkId,
				Block:  canonicalBlocks[0],
			})
		}
	}

	for forkId, fork := range cache.forkMap {
		if !forkParents[forkId] {
			forkHeads = append(forkHeads, &ForkHead{
				ForkId: forkId,
				Fork:   cache.forkMap[forkId],
				Block:  fork.headBlock,
			})
		}
	}

	return forkHeads
}

// getForksBefore retrieves all forks that happened before the given slot.
func (cache *forkCache) getForksBefore(slot phase0.Slot) []*Fork {
	cache.cacheMutex.RLock()
	defer cache.cacheMutex.RUnlock()

	var forks []*Fork
	for _, fork := range cache.forkMap {
		if fork.baseSlot < slot {
			forks = append(forks, fork)
		}
	}

	return forks
}

// setFinalizedEpoch sets the finalized epoch in the fork cache.
// It removes all forks that happened before the finalized epoch and updates the finalized fork ID.
func (cache *forkCache) setFinalizedEpoch(finalizedSlot phase0.Slot, justifiedRoot phase0.Root) {
	cache.cacheMutex.Lock()
	defer cache.cacheMutex.Unlock()

	for _, fork := range cache.forkMap {
		if fork.leafSlot >= finalizedSlot {
			continue
		}

		cache.parentIdCache.Add(fork.forkId, fork.parentFork)

		delete(cache.forkMap, fork.forkId)
	}

	latestFinalizedBlock := cache.indexer.blockCache.getBlockByRoot(justifiedRoot)
	finalizedForkId := ForkKey(0)
	for {
		if latestFinalizedBlock == nil {
			break
		}

		finalizedForkId = latestFinalizedBlock.forkId

		if latestFinalizedBlock.Slot <= finalizedSlot {
			break
		}

		parentRoot := latestFinalizedBlock.GetParentRoot()
		if parentRoot == nil {
			break
		}

		latestFinalizedBlock = cache.indexer.blockCache.getBlockByRoot(*parentRoot)
	}

	cache.finalizedForkId = finalizedForkId
	cache.parentIdsCache.Purge()

	err := db.RunDBTransaction(func(tx *sqlx.Tx) error {
		return cache.updateForkState(tx)
	})
	if err != nil {
		cache.indexer.logger.Errorf("error while updating fork state: %v", err)
	}
}
