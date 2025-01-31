package beacon

import (
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/ethpandaops/dora/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type beaconMetrics struct {
	indexer                    *Indexer
	blockCacheSlotMapSize      prometheus.Gauge
	blockCacheRootMapSize      prometheus.Gauge
	blockCacheParentMapSize    prometheus.Gauge
	blockCacheExecBlockMapSize prometheus.Gauge
	blockCacheBlockHeaderCount prometheus.Gauge
	blockCacheBlockBodyCount   prometheus.Gauge
	blockCacheBlockIndexCount  prometheus.Gauge

	epochCacheStatsMapSize      prometheus.Gauge
	epochCacheStateMapSize      prometheus.Gauge
	epochCacheStatsFullCount    prometheus.Gauge
	epochCacheStatsPrecalcCount prometheus.Gauge
	epochCacheStatsPrunedCount  prometheus.Gauge
	epochCacheStateLoadedCount  prometheus.Gauge
	epochCacheVotesCacheLen     prometheus.Gauge
	epochCacheVotesCacheHit     prometheus.Counter
	epochCacheVotesCacheMiss    prometheus.Counter

	forkCacheForkMapSize        prometheus.Gauge
	forkCacheParentIdCacheLen   prometheus.Gauge
	forkCacheParentIdCacheHit   prometheus.Counter
	forkCacheParentIdCacheMiss  prometheus.Counter
	forkCacheParentIdsCacheLen  prometheus.Gauge
	forkCacheParentIdsCacheHit  prometheus.Counter
	forkCacheParentIdsCacheMiss prometheus.Counter

	validatorCacheValidators        prometheus.Gauge
	validatorCacheValidatorDiffs    prometheus.Gauge
	validatorCacheValidatorData     prometheus.Gauge
	validatorCacheValidatorActivity prometheus.Gauge
	validatorCachePubkeyMapSize     prometheus.Gauge

	blockLoadDuration    prometheus.Histogram
	blockProcessDuration prometheus.Histogram
	blockStoreDuration   prometheus.Histogram

	epochStateLoadDuration     prometheus.Histogram
	epochStateLoadCount        prometheus.Counter
	epochStatsProcessDuration  prometheus.Histogram
	epochStatsStoreDuration    prometheus.Histogram
	epochStatsPackedSize       prometheus.Histogram
	epochVoteAggregateDuration prometheus.Histogram

	finalizationLoadDuration    prometheus.Histogram
	finalizationProcessDuration prometheus.Histogram
	finalizationStoreDuration   prometheus.Histogram
	finalizationCleanDuration   prometheus.Histogram

	pruningLoadDuration    prometheus.Histogram
	pruningProcessDuration prometheus.Histogram
	pruningStoreDuration   prometheus.Histogram
	pruningCleanDuration   prometheus.Histogram
}

func (indexer *Indexer) registerMetrics() *beaconMetrics {
	beaconMetrics := &beaconMetrics{
		indexer: indexer,
		blockCacheSlotMapSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_block_cache_slot_map_size",
			Help: "Number of entries in the block cache slot map",
		}),
		blockCacheRootMapSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_block_cache_root_map_size",
			Help: "Number of entries in the block cache root map",
		}),
		blockCacheParentMapSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_block_cache_parent_map_size",
			Help: "Number of entries in the block cache parent map",
		}),
		blockCacheExecBlockMapSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_block_cache_exec_block_map_size",
			Help: "Number of entries in the block cache exec block map",
		}),
		blockCacheBlockHeaderCount: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_block_cache_block_header_count",
			Help: "Number of blocks with a block header",
		}),
		blockCacheBlockBodyCount: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_block_cache_block_body_count",
			Help: "Number of blocks with a block body",
		}),
		blockCacheBlockIndexCount: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_block_cache_block_index_count",
			Help: "Number of blocks with a block index",
		}),

		epochCacheStatsMapSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_epoch_cache_stats_map_size",
			Help: "Number of entries in the epoch cache stats map",
		}),
		epochCacheStateMapSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_epoch_cache_state_map_size",
			Help: "Number of entries in the epoch cache state map",
		}),
		epochCacheStatsFullCount: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_epoch_cache_stats_full_count",
			Help: "Number of full epoch cache stats",
		}),
		epochCacheStatsPrecalcCount: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_epoch_cache_stats_precalc_count",
			Help: "Number of precalculated epoch cache stats",
		}),
		epochCacheStatsPrunedCount: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_epoch_cache_stats_pruned_count",
			Help: "Number of pruned epoch cache stats",
		}),
		epochCacheStateLoadedCount: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_epoch_cache_state_loaded_count",
			Help: "Number of loaded epoch cache state",
		}),
		epochCacheVotesCacheLen: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_epoch_cache_votes_cache_len",
			Help: "Number of entries in the epoch cache votes cache",
		}),
		epochCacheVotesCacheHit: promauto.NewCounter(prometheus.CounterOpts{
			Name: "dora_cl_indexer_epoch_cache_votes_cache_hit",
			Help: "Number of hits in the epoch cache votes cache",
		}),
		epochCacheVotesCacheMiss: promauto.NewCounter(prometheus.CounterOpts{
			Name: "dora_cl_indexer_epoch_cache_votes_cache_miss",
			Help: "Number of misses in the epoch cache votes cache",
		}),

		forkCacheForkMapSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_fork_cache_fork_map_size",
			Help: "Number of entries in the fork cache fork map",
		}),
		forkCacheParentIdCacheLen: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_fork_cache_parent_id_cache_len",
			Help: "Number of entries in the fork cache parent id cache",
		}),
		forkCacheParentIdCacheHit: promauto.NewCounter(prometheus.CounterOpts{
			Name: "dora_cl_indexer_fork_cache_parent_id_cache_hit",
			Help: "Number of hits in the fork cache parent id cache",
		}),
		forkCacheParentIdCacheMiss: promauto.NewCounter(prometheus.CounterOpts{
			Name: "dora_cl_indexer_fork_cache_parent_id_cache_miss",
			Help: "Number of misses in the fork cache parent id cache",
		}),
		forkCacheParentIdsCacheLen: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_fork_cache_parent_ids_cache_len",
			Help: "Number of entries in the fork cache parent ids cache",
		}),
		forkCacheParentIdsCacheHit: promauto.NewCounter(prometheus.CounterOpts{
			Name: "dora_cl_indexer_fork_cache_parent_ids_cache_hit",
			Help: "Number of hits in the fork cache parent ids cache",
		}),
		forkCacheParentIdsCacheMiss: promauto.NewCounter(prometheus.CounterOpts{
			Name: "dora_cl_indexer_fork_cache_parent_ids_cache_miss",
			Help: "Number of misses in the fork cache parent ids cache",
		}),
		validatorCacheValidators: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_validator_cache_validators",
			Help: "Number of validators in the validator cache",
		}),

		validatorCacheValidatorDiffs: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_validator_cache_validator_diffs",
			Help: "Number of validator diffs in the validator cache",
		}),
		validatorCacheValidatorData: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_validator_cache_validator_data",
			Help: "Number of validator data in the validator cache",
		}),
		validatorCacheValidatorActivity: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_validator_cache_validator_activity",
			Help: "Number of validator activity entries in the validator cache",
		}),
		validatorCachePubkeyMapSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dora_cl_indexer_validator_cache_pubkey_map_size",
			Help: "Number of entries in the validator cache pubkey map",
		}),

		blockLoadDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "dora_cl_indexer_block_load_duration",
			Help:    "Block loading time from clients",
			Buckets: []float64{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 250, 500, 750, 1000, 2500, 5000, 10000},
		}),
		blockProcessDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "dora_cl_indexer_block_process_duration",
			Help:    "Block processing time (fork detection, ...)",
			Buckets: []float64{0, 1, 2, 3, 4, 5, 10, 20, 50, 100, 250, 500, 1000},
		}),
		blockStoreDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "dora_cl_indexer_block_store_duration",
			Help:    "Block storing to db delay",
			Buckets: []float64{0, 1, 2, 3, 4, 5, 10, 20, 50, 100, 250, 500, 1000},
		}),

		epochStateLoadDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "dora_cl_indexer_epoch_state_load_duration",
			Help:    "Epoch state loading time from clients",
			Buckets: []float64{25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 10000, 20000, 30000, 60000, 90000, 120000},
		}),
		epochStateLoadCount: promauto.NewCounter(prometheus.CounterOpts{
			Name: "dora_cl_indexer_epoch_state_load_count",
			Help: "Number of epoch state loads from clients",
		}),
		epochStatsProcessDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "dora_cl_indexer_epoch_stats_process_duration",
			Help:    "Epoch stats processing time",
			Buckets: []float64{0, 1, 2, 3, 4, 5, 10, 20, 50, 100, 250, 500, 1000},
		}),
		epochStatsStoreDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "dora_cl_indexer_epoch_stats_store_duration",
			Help:    "Epoch stats storing to db delay",
			Buckets: []float64{0, 1, 2, 3, 4, 5, 10, 20, 50, 100, 250, 500, 1000},
		}),
		epochStatsPackedSize: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "dora_cl_indexer_epoch_stats_packed_size",
			Help:    "Size of the packed epoch stats in bytes",
			Buckets: []float64{0, 1000, 2500, 5000, 7500, 10000, 25000, 50000, 75000, 100000, 250000, 500000, 750000, 1000000, 2500000, 5000000, 7500000, 10000000, 25000000, 50000000, 75000000, 100000000},
		}),
		epochVoteAggregateDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "dora_cl_indexer_epoch_vote_aggregate_duration",
			Help:    "Epoch vote aggregation time",
			Buckets: []float64{0, 1, 2, 3, 4, 5, 10, 20, 50, 100, 250, 500, 1000},
		}),

		finalizationLoadDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "dora_cl_indexer_finalization_load_duration",
			Help:    "Finalization loading missing entities delay",
			Buckets: []float64{0, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 10000, 20000, 30000, 60000, 90000, 120000},
		}),
		finalizationProcessDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "dora_cl_indexer_finalization_process_duration",
			Help:    "Finalization processing time",
			Buckets: []float64{0, 1, 2, 3, 4, 5, 10, 20, 50, 100, 250, 500, 1000},
		}),
		finalizationStoreDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "dora_cl_indexer_finalization_store_duration",
			Help:    "Finalization storing to db delay",
			Buckets: []float64{0, 1, 2, 3, 4, 5, 10, 20, 50, 100, 250, 500, 1000},
		}),
		finalizationCleanDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "dora_cl_indexer_finalization_clean_duration",
			Help:    "Finalization cleaning time",
			Buckets: []float64{0, 1, 2, 3, 4, 5, 10, 20, 50, 100, 250, 500, 1000},
		}),

		pruningLoadDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "dora_cl_indexer_pruning_load_duration",
			Help:    "Pruning delay for loading missing entities from clients",
			Buckets: []float64{0, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 10000, 20000, 30000, 60000, 90000, 120000},
		}),
		pruningProcessDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "dora_cl_indexer_pruning_process_duration",
			Help:    "Pruning processing time",
			Buckets: []float64{0, 1, 2, 3, 4, 5, 10, 20, 50, 100, 250, 500, 1000},
		}),
		pruningStoreDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "dora_cl_indexer_pruning_store_duration",
			Help:    "Pruning storing to db delay",
			Buckets: []float64{0, 1, 2, 3, 4, 5, 10, 20, 50, 100, 250, 500, 1000},
		}),
		pruningCleanDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "dora_cl_indexer_pruning_clean_duration",
			Help:    "Pruning cleaning time",
			Buckets: []float64{0, 1, 2, 3, 4, 5, 10, 20, 50, 100, 250, 500, 1000},
		}),
	}

	metrics.AddPreCollectFn(func() {
		beaconMetrics.updateBlockCacheMetrics()
		beaconMetrics.updateEpochCacheMetrics()
		beaconMetrics.updateForkCacheMetrics()
		beaconMetrics.updateValidatorCacheMetrics()
	})

	return beaconMetrics
}

func (metrics *beaconMetrics) updateBlockCacheMetrics() {
	metrics.indexer.blockCache.cacheMutex.RLock()
	defer metrics.indexer.blockCache.cacheMutex.RUnlock()

	metrics.blockCacheSlotMapSize.Set(float64(len(metrics.indexer.blockCache.slotMap)))
	metrics.blockCacheRootMapSize.Set(float64(len(metrics.indexer.blockCache.rootMap)))
	metrics.blockCacheParentMapSize.Set(float64(len(metrics.indexer.blockCache.parentMap)))
	metrics.blockCacheExecBlockMapSize.Set(float64(len(metrics.indexer.blockCache.execBlockMap)))

	blockHeaderCount := 0
	blockBodyCount := 0
	blockIndexCount := 0
	for _, block := range metrics.indexer.blockCache.rootMap {
		if block.header != nil {
			blockHeaderCount++
		}
		if block.block != nil {
			blockBodyCount++
		}
		if block.blockIndex != nil {
			blockIndexCount++
		}
	}

	metrics.blockCacheBlockHeaderCount.Set(float64(blockHeaderCount))
	metrics.blockCacheBlockBodyCount.Set(float64(blockBodyCount))
	metrics.blockCacheBlockIndexCount.Set(float64(blockIndexCount))
}

func (metrics *beaconMetrics) updateEpochCacheMetrics() {
	metrics.indexer.epochCache.cacheMutex.RLock()
	defer metrics.indexer.epochCache.cacheMutex.RUnlock()

	metrics.epochCacheStatsMapSize.Set(float64(len(metrics.indexer.epochCache.statsMap)))
	metrics.epochCacheStateMapSize.Set(float64(len(metrics.indexer.epochCache.stateMap)))

	fullStatsCount := 0
	precalcStatsCount := 0
	prunedStatsCount := 0
	for _, stats := range metrics.indexer.epochCache.statsMap {
		if stats.values != nil {
			fullStatsCount++
		}
		if stats.precalcValues != nil {
			precalcStatsCount++
		}
		if stats.prunedValues != nil {
			prunedStatsCount++
		}
	}

	stateLoadedCount := 0
	for _, state := range metrics.indexer.epochCache.stateMap {
		if state.loadingStatus == 2 {
			stateLoadedCount++
		}
	}

	metrics.epochCacheStatsFullCount.Set(float64(fullStatsCount))
	metrics.epochCacheStatsPrecalcCount.Set(float64(precalcStatsCount))
	metrics.epochCacheStatsPrunedCount.Set(float64(prunedStatsCount))
	metrics.epochCacheStateLoadedCount.Set(float64(stateLoadedCount))
	metrics.epochCacheVotesCacheLen.Set(float64(metrics.indexer.epochCache.votesCache.Len()))
}

func (metrics *beaconMetrics) updateForkCacheMetrics() {
	metrics.indexer.forkCache.cacheMutex.RLock()
	defer metrics.indexer.forkCache.cacheMutex.RUnlock()

	metrics.forkCacheForkMapSize.Set(float64(len(metrics.indexer.forkCache.forkMap)))
	metrics.forkCacheParentIdCacheLen.Set(float64(metrics.indexer.forkCache.parentIdCache.Len()))
	metrics.forkCacheParentIdsCacheLen.Set(float64(metrics.indexer.forkCache.parentIdsCache.Len()))
}

func (metrics *beaconMetrics) updateValidatorCacheMetrics() {
	metrics.indexer.validatorCache.cacheMutex.RLock()
	defer metrics.indexer.validatorCache.cacheMutex.RUnlock()

	metrics.validatorCacheValidators.Set(float64(len(metrics.indexer.validatorCache.valsetCache)))

	validatorsMap := map[*phase0.Validator]bool{}
	validatorDiffs := 0
	for _, validator := range metrics.indexer.validatorCache.valsetCache {
		refs := len(validator.validatorDiffs)
		for _, diff := range validator.validatorDiffs {
			validatorsMap[diff.validator] = true
		}

		if validator.finalValidator != nil {
			validatorsMap[validator.finalValidator] = true
			refs++
		}
		validatorDiffs += refs
	}

	metrics.validatorCacheValidatorDiffs.Set(float64(validatorDiffs))
	metrics.validatorCacheValidatorData.Set(float64(len(validatorsMap)))

	activityCount := 0
	for _, recentActivity := range metrics.indexer.validatorCache.validatorActivityMap {
		activityCount += len(recentActivity)
	}
	metrics.validatorCacheValidatorActivity.Set(float64(activityCount))

	metrics.validatorCachePubkeyMapSize.Set(float64(len(metrics.indexer.validatorCache.pubkeyMap)))
}
