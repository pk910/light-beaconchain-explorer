package beacon

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/ethereum/go-ethereum/common/lru"

	"github.com/ethpandaops/dora/clients/consensus"
)

// epochStatsKey is the primary key for EpochStats entries in cache.
// consists of dependendRoot (32 byte) and epoch (8 byte).
type epochStatsKey [32 + 8]byte

// generate epochStatsKey from epoch and dependentRoot
func getEpochStatsKey(epoch phase0.Epoch, dependentRoot phase0.Root) epochStatsKey {
	var key epochStatsKey

	copy(key[0:], dependentRoot[:])
	binary.LittleEndian.PutUint64(key[32:], uint64(epoch))

	return key
}

// epochCache is the cache for EpochStats (epoch status) and epochState (beacon state) structures.
type epochCache struct {
	indexer        *Indexer
	cacheMutex     sync.RWMutex                  // mutex to protect statsMap & stateMap for concurrent read/write
	statsMap       map[epochStatsKey]*EpochStats // epoch status cache by epochStatsKey
	stateMap       map[phase0.Root]*epochState   // beacon state cache by dependentRoot
	loadingChan    chan bool                     // limits concurrent state calls by channel capacity
	syncMutex      sync.Mutex                    // mutex to protect syncCache for concurrent access
	syncCache      []phase0.ValidatorIndex       // global sync committee cache for reuse if matching
	precomputeLock sync.Mutex                    // mutex to prevent concurrent precomputing of epoch stats

	votesCache *lru.Cache[epochVotesKey, *EpochVotes] // cache for epoch vote aggregations
}

// newEpochCache creates & returns a new instance of epochCache.
// initializes the cache & starts the beacon state loader subroutine.
func newEpochCache(indexer *Indexer) *epochCache {
	cache := &epochCache{
		indexer:     indexer,
		statsMap:    map[epochStatsKey]*EpochStats{},
		stateMap:    map[phase0.Root]*epochState{},
		loadingChan: make(chan bool, indexer.maxParallelStateCalls),

		votesCache: lru.NewCache[epochVotesKey, *EpochVotes](500),
	}

	// start beacon state loader subroutine
	go cache.startLoaderLoop()

	return cache
}

// createOrGetEpochStats gets an existing EpochStats entry for the given epoch and dependentRoot or creates a new instance if not found.
func (cache *epochCache) createOrGetEpochStats(epoch phase0.Epoch, dependentRoot phase0.Root, createStateRequest bool) *EpochStats {
	cache.cacheMutex.Lock()
	defer cache.cacheMutex.Unlock()

	statsKey := getEpochStatsKey(epoch, dependentRoot)

	epochStats := cache.statsMap[statsKey]
	if epochStats == nil {
		epochStats = newEpochStats(epoch, dependentRoot)
		cache.statsMap[statsKey] = epochStats
	}

	// get or create beacon state which the epoch status depends on (dependentRoot beacon state)
	epochState := cache.stateMap[dependentRoot]
	if epochState == nil && !epochStats.ready && createStateRequest {
		epochState = newEpochState(dependentRoot)
		cache.stateMap[dependentRoot] = epochState

		cache.indexer.logger.Infof("added epoch state request for epoch %v (%v) to queue", epoch, dependentRoot.String())
	}

	if epochState != nil {
		epochStats.dependentState = epochState

		if epochState.loadingStatus == 2 && !epochStats.ready {
			// dependent state is already loaded, process it
			go epochStats.processState(cache.indexer, nil)
		}
	}

	return epochStats
}

func (cache *epochCache) addEpochStateRequest(epochStats *EpochStats) {
	if epochStats.dependentState != nil {
		return
	}

	cache.cacheMutex.Lock()
	defer cache.cacheMutex.Unlock()

	epochState := cache.stateMap[epochStats.dependentRoot]
	if epochState == nil {
		epochState = newEpochState(epochStats.dependentRoot)
		cache.stateMap[epochStats.dependentRoot] = epochState

		cache.indexer.logger.Infof("added epoch state request for epoch %v (%v) to queue", epochStats.epoch, epochStats.dependentRoot.String())
	}
	epochStats.dependentState = epochState
}

func (cache *epochCache) getEpochStats(epoch phase0.Epoch, dependentRoot phase0.Root) *EpochStats {
	cache.cacheMutex.RLock()
	defer cache.cacheMutex.RUnlock()

	statsKey := getEpochStatsKey(epoch, dependentRoot)

	return cache.statsMap[statsKey]
}

// getPendingEpochStats gets all EpochStats with unloaded epochStates.
func (cache *epochCache) getPendingEpochStats() []*EpochStats {
	cache.cacheMutex.Lock()
	defer cache.cacheMutex.Unlock()

	pendingStats := make([]*EpochStats, 0)
	for _, stats := range cache.statsMap {
		if stats.dependentState != nil && stats.dependentState.loadingStatus == 0 {
			pendingStats = append(pendingStats, stats)
		}
	}

	return pendingStats
}

func (cache *epochCache) getEpochStatsByEpoch(epoch phase0.Epoch) []*EpochStats {
	cache.cacheMutex.RLock()
	defer cache.cacheMutex.RUnlock()

	statsList := []*EpochStats{}
	for _, stats := range cache.statsMap {
		if stats.epoch == epoch {
			statsList = append(statsList, stats)
		}
	}

	return statsList
}

func (cache *epochCache) getEpochStatsBeforeEpoch(epoch phase0.Epoch) []*EpochStats {
	cache.cacheMutex.RLock()
	defer cache.cacheMutex.RUnlock()

	statsList := []*EpochStats{}
	for _, stats := range cache.statsMap {
		if stats.epoch < epoch {
			statsList = append(statsList, stats)
		}
	}

	return statsList
}

// removeEpochStats removes an EpochStats struct from cache.
// stops loading state call if not referenced by another epoch status.
func (cache *epochCache) removeEpochStats(epochStats *EpochStats) {
	cache.cacheMutex.Lock()
	defer cache.cacheMutex.Unlock()

	statsKey := getEpochStatsKey(epochStats.epoch, epochStats.dependentRoot)

	if cache.statsMap[statsKey] == nil {
		return
	}

	delete(cache.statsMap, statsKey)

	if epochStats.dependentState != nil {
		foundOtherStats := false
		for _, stats := range cache.statsMap {
			if bytes.Equal(stats.dependentRoot[:], epochStats.dependentRoot[:]) {
				foundOtherStats = true
				break
			}
		}

		if !foundOtherStats {
			// no other epoch status depends on this beacon state
			epochStats.dependentState.dispose()
			delete(cache.stateMap, epochStats.dependentRoot)
		}
	}
}

func (cache *epochCache) removeEpochStatsByEpoch(epoch phase0.Epoch) {
	for _, stats := range cache.getEpochStatsByEpoch(epoch) {
		cache.removeEpochStats(stats)
	}
}

func (cache *epochCache) removeUnreferencedEpochStates() uint64 {
	cache.cacheMutex.Lock()
	defer cache.cacheMutex.Unlock()

	removed := uint64(0)
	for _, state := range cache.stateMap {
		found := false
		for _, stats := range cache.statsMap {
			if stats.dependentState == state {
				found = true
				break
			}
		}

		if !found {
			state.dispose()
			delete(cache.stateMap, state.slotRoot)
			removed++
		}
	}

	return removed
}

// getOrUpdateSyncCommittee replaces the supplied sync committee with an older sync committee from cache if all properties match.
// heavily reduces memory consumption as sync committee objects are not duplicated for each sync committee request.
func (cache *epochCache) getOrUpdateSyncCommittee(syncCommittee []phase0.ValidatorIndex) []phase0.ValidatorIndex {
	cache.syncMutex.Lock()
	defer cache.syncMutex.Unlock()

	isEqual := false
	if len(syncCommittee) == len(cache.syncCache) {
		isEqual = true
		for i, index := range syncCommittee {
			if cache.syncCache[i] != index {
				isEqual = false
				break
			}
		}
	}

	if isEqual {
		// all properties match, return reference to old cached entry
		return cache.syncCache
	}

	cache.syncCache = syncCommittee
	return syncCommittee
}

func (cache *epochCache) withPrecomputeLock(f func() error) error {
	cache.precomputeLock.Lock()
	defer cache.precomputeLock.Unlock()

	return f()
}

// startLoaderLoop is the entrypoint for the beacon state loader subroutine.
// contains the main loop & crash handler of the subroutine.
func (cache *epochCache) startLoaderLoop() {
	defer func() {
		if err := recover(); err != nil {
			cache.indexer.logger.WithError(err.(error)).Errorf("uncaught panic in indexer.beacon.epochCache.startLoaderLoop subroutine: %v, stack: %v", err, string(debug.Stack()))
			time.Sleep(10 * time.Second)

			go cache.startLoaderLoop()
		}
	}()

	for {
		cache.runLoaderLoop()
		time.Sleep(2 * time.Second)
	}
}

// runLoaderLoop checks the cache for unloaded epoch states.
// loads the next unloaded state in a subroutine if needed.
// blocks if too many loader subroutines are already running.
func (cache *epochCache) runLoaderLoop() {
	// load next epoch stats
	pendingStats := cache.getPendingEpochStats()
	if len(pendingStats) == 0 {
		return
	}

	// sort by loading priority
	// 1. bad states (prefer <= 10 retries)
	// 2. high priority states (most recent 2 epochs are always high priority)
	// 3. retry count (prefer lower)
	// 4. requested by clients count (prefer higher)
	// 5. epoch number (prefer higher)
	currentEpoch := cache.indexer.consensusPool.GetChainState().CurrentEpoch()
	sort.Slice(pendingStats, func(a, b int) bool {
		probablyBadA := pendingStats[a].dependentState.retryCount > beaconStateRetryCount
		probablyBadB := pendingStats[b].dependentState.retryCount > beaconStateRetryCount
		if probablyBadA != probablyBadB {
			return probablyBadB
		}

		highPriorityA := pendingStats[a].dependentState.highPriority || currentEpoch < 2 || pendingStats[a].epoch >= currentEpoch-2
		highPriorityB := pendingStats[b].dependentState.highPriority || currentEpoch < 2 || pendingStats[b].epoch >= currentEpoch-2
		if highPriorityA != highPriorityB {
			return highPriorityA
		}

		if pendingStats[a].dependentState.retryCount != pendingStats[b].dependentState.retryCount {
			return pendingStats[a].dependentState.retryCount < pendingStats[b].dependentState.retryCount
		}

		if pendingStats[a].dependentState.retryCount != pendingStats[b].dependentState.retryCount {
			return pendingStats[a].dependentState.retryCount < pendingStats[b].dependentState.retryCount
		}

		reqCountA := len(pendingStats[a].requestedBy)
		reqCountB := len(pendingStats[b].requestedBy)
		if reqCountA != reqCountB {
			return reqCountA > reqCountB
		}

		return pendingStats[a].epoch > pendingStats[b].epoch
	})

	if cache.indexer.maxParallelStateCalls > 0 {
		cache.loadingChan <- true
	}

	go func() {
		defer func() {
			if cache.indexer.maxParallelStateCalls > 0 {
				<-cache.loadingChan
			}
		}()

		for _, pendingStats := range pendingStats {
			if cache.loadEpochStats(pendingStats) {
				break
			}
		}
	}()
}

// loadEpochStats loads the supplied unloaded epoch status (the dependent epoch state).
// retires loading from multiple clients, ordered by priority.
// returns true if a epoch state request was done (either successful or failed).
func (cache *epochCache) loadEpochStats(epochStats *EpochStats) bool {
	defer func() {
		if err := recover(); err != nil {
			cache.indexer.logger.WithError(err.(error)).Errorf("uncaught panic in indexer.beacon.epochCache.loadEpochStats subroutine: %v, stack: %v", err, string(debug.Stack()))
		}
	}()

	clients := []*Client{}
	preferArchive := epochStats.epoch < cache.indexer.lastFinalizedEpoch
	for _, client := range cache.indexer.GetReadyClientsByBlockRoot(epochStats.dependentRoot, preferArchive) {
		if client.skipValidators {
			continue
		}

		if client.client.GetStatus() != consensus.ClientStatusOnline && client.client.GetStatus() != consensus.ClientStatusOptimistic {
			continue
		}

		if !cache.indexer.blockCache.isCanonicalBlock(epochStats.dependentRoot, client.headRoot) {
			continue
		}

		clients = append(clients, client)
	}

	if len(clients) == 0 {
		for _, client := range epochStats.getRequestedBy() {
			if client.skipValidators {
				continue
			}

			if client.client.GetStatus() != consensus.ClientStatusOnline && client.client.GetStatus() != consensus.ClientStatusOptimistic {
				continue
			}

			clients = append(clients, client)
		}
	}

	if len(clients) == 0 {
		cache.indexer.logger.Debugf("no clients available to load epoch %v stats (dep: %v)", epochStats.epoch, epochStats.dependentRoot.String())
		epochStats.dependentState.retryCount++
		return false
	}

	sort.Slice(clients, func(a, b int) bool {
		cliA := clients[a]
		cliB := clients[b]

		if cliA.archive != cliB.archive {
			if cliA.archive {
				return false
			} else {
				return true
			}
		}

		if cliA.priority != cliB.priority {
			if cliA.priority > cliB.priority {
				return false
			} else {
				return true
			}
		}

		hashA := md5.Sum([]byte(fmt.Sprintf("%v-%v", cliA.client.GetIndex(), epochStats.epoch)))
		hashB := md5.Sum([]byte(fmt.Sprintf("%v-%v", cliB.client.GetIndex(), epochStats.epoch)))
		return bytes.Compare(hashA[:], hashB[:]) < 0
	})

	client := clients[int(epochStats.dependentState.retryCount)%len(clients)]
	log := cache.indexer.logger.WithField("client", client.client.GetName())
	if epochStats.dependentState.retryCount > 0 {
		log = log.WithField("retry", epochStats.dependentState.retryCount)
	}

	log.Infof("loading epoch %v stats (dep: %v, req: %v)", epochStats.epoch, epochStats.dependentRoot.String(), len(epochStats.requestedBy))

	state, err := epochStats.dependentState.loadState(client.getContext(), client, cache)
	if err != nil && epochStats.dependentState.loadingStatus == 0 {
		client.logger.Warnf("failed loading epoch %v stats (dep: %v): %v", epochStats.epoch, epochStats.dependentRoot.String(), err)
	}

	if epochStats.dependentState.loadingStatus != 2 {
		// epoch state could not be loaded
		epochStats.dependentState.retryCount++
		return false
	}

	var validatorSet []*phase0.Validator
	if state != nil {
		validatorSet, err = state.Validators()
		if err != nil {
			cache.indexer.logger.Errorf("error getting validator set from state %v: %v", epochStats.dependentRoot.String(), err)
		}
	}

	dependentStats := []*EpochStats{}
	cache.cacheMutex.Lock()
	for _, stats := range cache.statsMap {
		if stats.dependentState == epochStats.dependentState {
			dependentStats = append(dependentStats, stats)
		}
	}
	cache.cacheMutex.Unlock()

	for _, stats := range dependentStats {
		go stats.processState(cache.indexer, validatorSet)
	}

	return true
}
