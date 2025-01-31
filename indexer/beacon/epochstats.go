package beacon

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/ethpandaops/dora/clients/consensus"
	"github.com/ethpandaops/dora/db"
	"github.com/ethpandaops/dora/dbtypes"
	"github.com/ethpandaops/dora/indexer/beacon/duties"
	"github.com/jmoiron/sqlx"
	"github.com/mashingan/smapping"
	dynssz "github.com/pk910/dynamic-ssz"
)

// EpochStats holds the epoch-specific information based on the underlying dependent beacon state.
type EpochStats struct {
	epoch          phase0.Epoch
	dependentRoot  phase0.Root
	dependentState *epochState

	requestedMutex  sync.Mutex
	requestedBy     []*Client
	ready           bool
	readyChanMutex  sync.Mutex
	readyChan       chan bool
	processingMutex sync.Mutex
	processing      bool
	isInDb          bool

	precalcBaseRoot phase0.Root
	precalcValues   *EpochStatsValues
	values          *EpochStatsValues
	prunedValues    *EpochStatsValues

	prunedEpochAggregations []*dbtypes.UnfinalizedEpoch
}

// EpochStatsValues holds the values for the epoch-specific information.
type EpochStatsValues struct {
	RandaoMix           phase0.Hash32
	NextRandaoMix       phase0.Hash32
	ActiveIndices       []phase0.ValidatorIndex
	EffectiveBalances   []uint16
	ProposerDuties      []phase0.ValidatorIndex
	AttesterDuties      [][][]duties.ActiveIndiceIndex
	SyncCommitteeDuties []phase0.ValidatorIndex
	ActiveValidators    uint64
	TotalBalance        phase0.Gwei
	ActiveBalance       phase0.Gwei
	EffectiveBalance    phase0.Gwei
	FirstDepositIndex   uint64
}

// EpochStatsPacked holds the packed values for the epoch-specific information.
type EpochStatsPacked struct {
	ActiveValidators    []EpochStatsPackedValidator
	ProposerDuties      []phase0.ValidatorIndex
	SyncCommitteeDuties []phase0.ValidatorIndex
	RandaoMix           phase0.Hash32
	NextRandaoMix       phase0.Hash32
	TotalBalance        phase0.Gwei
	ActiveBalance       phase0.Gwei
	FirstDepositIndex   uint64
}

// EpochStatsPackedValidator holds the packed values for an active validator.
type EpochStatsPackedValidator struct {
	ValidatorIndexOffset uint32 // offset to the previous index in the list (this is smaller than storing the full validator index)
	EffectiveBalanceEth  uint16 // effective balance in full ETH
}

// newEpochStats creates a new EpochStats instance.
func newEpochStats(epoch phase0.Epoch, dependentRoot phase0.Root) *EpochStats {
	stats := &EpochStats{
		epoch:         epoch,
		dependentRoot: dependentRoot,
		requestedBy:   make([]*Client, 0),
	}

	return stats
}

func (es *EpochStats) GetEpoch() phase0.Epoch {
	return es.epoch
}

func (es *EpochStats) GetDependentRoot() phase0.Root {
	return es.dependentRoot
}

// addRequestedBy adds a client to the list of clients that have requested this EpochStats.
func (es *EpochStats) addRequestedBy(client *Client) bool {
	es.requestedMutex.Lock()
	defer es.requestedMutex.Unlock()

	for _, c := range es.requestedBy {
		if c == client {
			return false
		}
	}

	es.requestedBy = append(es.requestedBy, client)
	return true
}

// getRequestedBy returns a copy of the list of clients that have requested this EpochStats.
func (es *EpochStats) getRequestedBy() []*Client {
	es.requestedMutex.Lock()
	defer es.requestedMutex.Unlock()

	clients := make([]*Client, len(es.requestedBy))
	copy(clients, es.requestedBy)

	return clients
}

func (es *EpochStats) restoreFromDb(dbDuty *dbtypes.UnfinalizedDuty, dynSsz *dynssz.DynSsz, chainState *consensus.ChainState, withDuties bool) error {
	if es.ready {
		return nil
	}

	values, err := es.parsePackedSSZ(dynSsz, chainState, dbDuty.DutiesSSZ, withDuties)
	if err != nil {
		return err
	}

	es.values = values
	es.setStatsReady()

	return nil
}

func (es *EpochStats) setStatsReady() {
	es.readyChanMutex.Lock()
	defer es.readyChanMutex.Unlock()

	es.ready = true
	if es.readyChan != nil {
		close(es.readyChan)
		es.readyChan = nil
	}
}

// marshalSSZ marshals the EpochStats values using SSZ.
func (es *EpochStats) buildPackedSSZ(dynSsz *dynssz.DynSsz) ([]byte, error) {
	if es.values == nil {
		return nil, fmt.Errorf("no values to marshal")
	}

	if dynSsz == nil {
		dynSsz = dynssz.NewDynSsz(nil)
	}

	packedValues := &EpochStatsPacked{
		ActiveValidators:    make([]EpochStatsPackedValidator, es.values.ActiveValidators),
		ProposerDuties:      es.values.ProposerDuties,
		SyncCommitteeDuties: es.values.SyncCommitteeDuties,
		RandaoMix:           es.values.RandaoMix,
		NextRandaoMix:       es.values.NextRandaoMix,
		TotalBalance:        es.values.TotalBalance,
		ActiveBalance:       es.values.ActiveBalance,
		FirstDepositIndex:   es.values.FirstDepositIndex,
	}

	lastValidatorIndex := phase0.ValidatorIndex(0)
	for i, validatorIndex := range es.values.ActiveIndices {
		validatorOffset := uint32(validatorIndex - lastValidatorIndex)
		lastValidatorIndex = validatorIndex

		packedValues.ActiveValidators[i] = EpochStatsPackedValidator{
			ValidatorIndexOffset: validatorOffset,
			EffectiveBalanceEth:  es.values.EffectiveBalances[i],
		}
	}

	rawSsz, err := dynSsz.MarshalSSZ(packedValues)
	if err != nil {
		return nil, err
	}

	return compressBytes(rawSsz), nil
}

// unmarshalSSZ unmarshals the EpochStats values using the provided SSZ bytes.
// skips computing attester duties if withCommittees is false to speed up the process.
func (es *EpochStats) parsePackedSSZ(dynSsz *dynssz.DynSsz, chainState *consensus.ChainState, ssz []byte, withDuties bool) (*EpochStatsValues, error) {
	if dynSsz == nil {
		dynSsz = dynssz.NewDynSsz(nil)
	}

	if len(ssz) == 0 {
		return nil, nil
	}

	if d, err := decompressBytes(ssz); err != nil {
		return nil, err
	} else {
		ssz = d
	}

	packedValues := &EpochStatsPacked{}
	if err := dynSsz.UnmarshalSSZ(packedValues, ssz); err != nil {
		return nil, err
	}

	values := &EpochStatsValues{
		RandaoMix:           packedValues.RandaoMix,
		NextRandaoMix:       packedValues.NextRandaoMix,
		ActiveIndices:       make([]phase0.ValidatorIndex, len(packedValues.ActiveValidators)),
		EffectiveBalances:   make([]uint16, len(packedValues.ActiveValidators)),
		ProposerDuties:      packedValues.ProposerDuties,
		SyncCommitteeDuties: packedValues.SyncCommitteeDuties,
		TotalBalance:        packedValues.TotalBalance,
		ActiveBalance:       packedValues.ActiveBalance,
		EffectiveBalance:    0,
		FirstDepositIndex:   packedValues.FirstDepositIndex,
	}

	lastValidatorIndex := phase0.ValidatorIndex(0)
	for i, packedValidator := range packedValues.ActiveValidators {
		validatorIndex := lastValidatorIndex + phase0.ValidatorIndex(packedValidator.ValidatorIndexOffset)
		lastValidatorIndex = validatorIndex

		values.EffectiveBalances[i] = packedValidator.EffectiveBalanceEth
		values.EffectiveBalance += phase0.Gwei(packedValidator.EffectiveBalanceEth) * EtherGweiFactor
		values.ActiveIndices[i] = validatorIndex
	}

	values.ActiveValidators = uint64(len(packedValues.ActiveValidators))

	if withDuties {
		beaconState := &duties.BeaconState{
			RandaoMix: &values.RandaoMix,
			GetActiveCount: func() uint64 {
				return values.ActiveValidators
			},
			GetEffectiveBalance: func(index duties.ActiveIndiceIndex) phase0.Gwei {
				return phase0.Gwei(values.EffectiveBalances[index]) * EtherGweiFactor
			},
		}

		// compute proposers
		proposerDuties := []phase0.ValidatorIndex{}
		for slot := chainState.EpochToSlot(es.epoch); slot < chainState.EpochToSlot(es.epoch+1); slot++ {
			proposer, err := duties.GetProposerIndex(chainState.GetSpecs(), beaconState, slot)
			proposerIndex := phase0.ValidatorIndex(math.MaxInt64)
			if err == nil {
				proposerIndex = values.ActiveIndices[proposer]
			}

			proposerDuties = append(proposerDuties, proposerIndex)
		}

		values.ProposerDuties = proposerDuties
		if beaconState.RandaoMix != nil {
			values.RandaoMix = *beaconState.RandaoMix
		}

		// compute committees
		attesterDuties, _ := duties.GetAttesterDuties(chainState.GetSpecs(), beaconState, es.epoch)
		values.AttesterDuties = attesterDuties
	}

	return values, nil
}

// packValues packs the EpochStats values.
func (es *EpochStats) pruneValues() {
	if es.values == nil {
		return
	}

	es.prunedValues = &EpochStatsValues{
		RandaoMix:           es.values.RandaoMix,
		NextRandaoMix:       es.values.NextRandaoMix,
		EffectiveBalances:   nil, // prune
		ProposerDuties:      es.values.ProposerDuties,
		AttesterDuties:      nil, // prune
		SyncCommitteeDuties: es.values.SyncCommitteeDuties,
		ActiveValidators:    es.values.ActiveValidators,
		TotalBalance:        es.values.TotalBalance,
		ActiveBalance:       es.values.ActiveBalance,
		EffectiveBalance:    es.values.EffectiveBalance,
		FirstDepositIndex:   es.values.FirstDepositIndex,
	}

	es.values = nil
}

func (es *EpochStats) loadValuesFromDb(dynSsz *dynssz.DynSsz, chainState *consensus.ChainState) *EpochStatsValues {
	if !es.isInDb {
		return nil
	}

	dbDuty := db.GetUnfinalizedDuty(uint64(es.epoch), es.dependentRoot[:])
	if dbDuty == nil {
		return nil
	}

	values, err := es.parsePackedSSZ(dynSsz, chainState, dbDuty.DutiesSSZ, true)
	if err != nil {
		return nil
	}

	return values
}

// processState processes the epoch state and computes proposer and attester duties.
func (es *EpochStats) processState(indexer *Indexer, validatorSet []*phase0.Validator) {
	if es.dependentState == nil || es.dependentState.loadingStatus != 2 {
		return
	}

	es.processingMutex.Lock()
	if es.processing {
		es.processingMutex.Unlock()
		return
	}

	es.processing = true
	es.processingMutex.Unlock()

	defer func() {
		es.processing = false
	}()

	t1 := time.Now()

	chainState := indexer.consensusPool.GetChainState()
	values := &EpochStatsValues{
		ActiveIndices:       make([]phase0.ValidatorIndex, 0),
		EffectiveBalances:   make([]uint16, 0),
		SyncCommitteeDuties: es.dependentState.syncCommittee,
		TotalBalance:        0,
		ActiveBalance:       0,
		EffectiveBalance:    0,
		FirstDepositIndex:   es.dependentState.depositIndex,
	}

	if validatorSet == nil {
		validatorSet = indexer.validatorCache.getValidatorSetForRoot(es.dependentRoot)
	}

	// get active validator indices & aggregate balances
	for index, validator := range validatorSet {
		values.TotalBalance += es.dependentState.validatorBalances[index]
		if es.epoch >= validator.ActivationEpoch && es.epoch < validator.ExitEpoch {
			values.ActiveIndices = append(values.ActiveIndices, phase0.ValidatorIndex(index))
			values.EffectiveBalances = append(values.EffectiveBalances, uint16(validator.EffectiveBalance/EtherGweiFactor))
			values.EffectiveBalance += validator.EffectiveBalance
			values.ActiveBalance += es.dependentState.validatorBalances[index]
		}
	}

	values.ActiveValidators = uint64(len(values.ActiveIndices))
	beaconState := &duties.BeaconState{
		GetRandaoMixes: func() []phase0.Root {
			return es.dependentState.randaoMixes
		},
		GetActiveCount: func() uint64 {
			return values.ActiveValidators
		},
		GetEffectiveBalance: func(index duties.ActiveIndiceIndex) phase0.Gwei {
			return phase0.Gwei(values.EffectiveBalances[index]) * EtherGweiFactor
		},
	}

	indexer.logger.Debugf("processing epoch %v stats (root: %v / state: %v), validators: %v/%v", es.epoch, es.dependentRoot.String(), es.dependentState.stateRoot.String(), values.ActiveValidators, len(validatorSet))

	// compute proposers
	proposerDuties := []phase0.ValidatorIndex{}
	for slot := chainState.EpochToSlot(es.epoch); slot < chainState.EpochToSlot(es.epoch+1); slot++ {
		proposer, err := duties.GetProposerIndex(chainState.GetSpecs(), beaconState, slot)
		proposerIndex := phase0.ValidatorIndex(math.MaxInt64)
		if err != nil {
			indexer.logger.Warnf("failed computing proposer for slot %v: %v", slot, err)
			proposerIndex = math.MaxInt64
		} else {
			proposerIndex = values.ActiveIndices[proposer]
		}

		proposerDuties = append(proposerDuties, proposerIndex)
	}

	values.ProposerDuties = proposerDuties
	if beaconState.RandaoMix != nil {
		values.RandaoMix = *beaconState.RandaoMix
		values.NextRandaoMix = *beaconState.NextRandaoMix
	}

	// compute committees
	attesterDuties, err := duties.GetAttesterDuties(chainState.GetSpecs(), beaconState, es.epoch)
	if err != nil {
		indexer.logger.Warnf("failed computing attester duties for epoch %v: %v", es.epoch, err)
	}
	values.AttesterDuties = attesterDuties

	es.values = values
	es.precalcValues = nil

	packedSsz, _ := es.buildPackedSSZ(indexer.dynSsz)
	dbDuty := &dbtypes.UnfinalizedDuty{
		Epoch:         uint64(es.epoch),
		DependentRoot: es.dependentRoot[:],
		DutiesSSZ:     packedSsz,
	}

	t1dur := time.Since(t1)
	t1 = time.Now()

	err = db.RunDBTransaction(func(tx *sqlx.Tx) error {
		return db.InsertUnfinalizedDuty(dbDuty, tx)
	})
	if err != nil {
		indexer.logger.WithError(err).Errorf("failed storing epoch %v stats (%v / %v) to unfinalized duties", es.epoch, es.dependentRoot.String(), es.dependentState.stateRoot.String())
	}

	t2dur := time.Since(t1)

	es.isInDb = true

	indexer.metrics.epochStatsProcessDuration.Observe(float64(t1dur.Milliseconds()))
	indexer.metrics.epochStatsStoreDuration.Observe(float64(t2dur.Milliseconds()))
	indexer.metrics.epochStatsPackedSize.Observe(float64(len(packedSsz)))

	indexer.logger.Infof(
		"processed epoch %v stats (root: %v / state: %v, validators: %v/%v, %v ms), %v bytes",
		es.epoch,
		es.dependentRoot.String(),
		es.dependentState.stateRoot.String(),
		values.ActiveValidators,
		len(validatorSet),
		(t1dur + t2dur).Milliseconds(),
		len(packedSsz),
	)

	es.setStatsReady()
}

// precomputeFromParentState precomputes the EpochStats values based on the parent state.
func (es *EpochStats) precomputeFromParentState(indexer *Indexer, parentState *EpochStats) error {
	es.precalcBaseRoot = parentState.dependentRoot

	return indexer.epochCache.withPrecomputeLock(func() error {
		if es.precalcValues != nil {
			return nil
		}
		t1 := time.Now()

		otherEpochStats := indexer.epochCache.getEpochStatsByEpoch(es.epoch)
		for _, other := range otherEpochStats {
			if other == es {
				continue
			}

			if other.precalcValues != nil && bytes.Equal(other.precalcBaseRoot[:], parentState.dependentRoot[:]) {
				es.precalcValues = other.precalcValues
				return nil
			}
		}

		parentStatsValues := parentState.GetValues(false)
		if parentStatsValues == nil {
			return fmt.Errorf("parent stats values not available")
		}

		values := &EpochStatsValues{
			ActiveIndices:       parentStatsValues.ActiveIndices,
			RandaoMix:           parentStatsValues.NextRandaoMix,
			EffectiveBalances:   parentStatsValues.EffectiveBalances,
			SyncCommitteeDuties: parentStatsValues.SyncCommitteeDuties,
			TotalBalance:        parentStatsValues.TotalBalance,
			ActiveBalance:       parentStatsValues.ActiveBalance,
			EffectiveBalance:    parentStatsValues.EffectiveBalance,
		}

		values.ActiveValidators = uint64(len(values.ActiveIndices))

		beaconState := &duties.BeaconState{
			RandaoMix: &values.RandaoMix,
			GetActiveCount: func() uint64 {
				return values.ActiveValidators
			},
			GetEffectiveBalance: func(index duties.ActiveIndiceIndex) phase0.Gwei {
				return phase0.Gwei(values.EffectiveBalances[index]) * EtherGweiFactor
			},
		}
		chainState := indexer.consensusPool.GetChainState()

		// compute proposers
		proposerDuties := []phase0.ValidatorIndex{}
		for slot := chainState.EpochToSlot(es.epoch); slot < chainState.EpochToSlot(es.epoch+1); slot++ {
			proposer, err := duties.GetProposerIndex(chainState.GetSpecs(), beaconState, slot)
			proposerIndex := phase0.ValidatorIndex(math.MaxInt64)
			if err == nil {
				proposerIndex = values.ActiveIndices[proposer]
			}

			proposerDuties = append(proposerDuties, proposerIndex)
		}

		values.ProposerDuties = proposerDuties

		// compute committees
		attesterDuties, _ := duties.GetAttesterDuties(chainState.GetSpecs(), beaconState, es.epoch)
		values.AttesterDuties = attesterDuties

		es.precalcValues = values

		indexer.logger.Infof(
			"precomputed epoch %v stats (root: %v), validators: %v/%v (%v ms)",
			es.epoch,
			es.dependentRoot.String(),
			values.ActiveValidators,
			len(parentStatsValues.EffectiveBalances),
			time.Since(t1).Milliseconds(),
		)

		return nil
	})
}

// awaitStatsReady waits for the EpochStats values to be ready.
func (s *EpochStats) awaitStatsReady(ctx context.Context, timeout time.Duration) bool {
	s.readyChanMutex.Lock()
	if s.readyChan == nil && !s.ready {
		s.readyChan = make(chan bool)
	}
	s.readyChanMutex.Unlock()

	timeoutTime := time.Now().Add(timeout)
	for {
		if s.ready {
			return true
		}

		select {
		case <-s.readyChan:
			return true
		case <-time.After(time.Until(timeoutTime)):
			return false
		case <-ctx.Done():
			return false
		}
	}
}

// GetValues returns the EpochStats values.
func (es *EpochStats) GetValues(withPrecalc bool) *EpochStatsValues {
	if es == nil {
		return nil
	}

	if es.values != nil {
		return es.values
	}

	if es.prunedValues != nil {
		return es.prunedValues
	}

	if es.precalcValues != nil && withPrecalc {
		return es.precalcValues
	}

	return nil
}

// GetOrLoadValues returns the EpochStats values, loading them from the database if necessary.
func (es *EpochStats) GetOrLoadValues(indexer *Indexer, withPrecalc bool, keepInCache bool) *EpochStatsValues {
	if es == nil {
		return nil
	}

	if es.values != nil {
		return es.values
	}

	if es.isInDb {
		values := es.loadValuesFromDb(indexer.dynSsz, indexer.consensusPool.GetChainState())
		if values != nil {
			if keepInCache {
				es.values = values
			}
			return values
		}
	}

	if es.precalcValues != nil && withPrecalc {
		return es.precalcValues
	}

	return nil
}

// GetEffectiveBalance returns the effective balance for the given active validator indice.
func (v *EpochStatsValues) GetEffectiveBalance(index duties.ActiveIndiceIndex) phase0.Gwei {
	if v == nil {
		return 0
	}

	return phase0.Gwei(v.EffectiveBalances[index]) * EtherGweiFactor
}

// GetDbEpoch returns the database Epoch representaion for the EpochStats.
func (es *EpochStats) GetDbEpoch(indexer *Indexer, headBlock *Block) *dbtypes.Epoch {
	chainState := indexer.consensusPool.GetChainState()
	if headBlock == nil {
		headBlock = indexer.GetCanonicalHead(nil)
	}

	// collect all blocks for this & next epoch in chain defined by headBlock
	epochBlockMap := map[phase0.Root]bool{}
	epochBlocks := []*Block{}
	currentBlock := headBlock
	for {
		if currentBlock == nil || chainState.EpochOfSlot(currentBlock.Slot) < es.epoch {
			break
		}

		if chainState.EpochOfSlot(currentBlock.Slot) == es.epoch {
			epochBlockMap[currentBlock.Root] = true
			epochBlocks = append(epochBlocks, currentBlock)
		}

		parentRoot := currentBlock.GetParentRoot()
		if parentRoot == nil {
			break
		}

		currentBlock = indexer.blockCache.getBlockByRoot(*parentRoot)
	}

	if es.prunedEpochAggregations != nil {
		// select from the pruned epoch aggregations
		for _, epochAgg := range es.prunedEpochAggregations {
			if !epochBlockMap[phase0.Root(epochAgg.EpochHeadRoot)] {
				continue
			}

			mapped := smapping.MapTags(epochAgg, "db")

			dbEpoch := &dbtypes.Epoch{}
			err := smapping.FillStructByTags(dbEpoch, mapped, "db")
			if err != nil {
				indexer.logger.Errorf("mapper failed copying unfinalized epoch to epoch: %v", err)
				continue
			}

			return dbEpoch
		}

		if len(epochBlocks) > 0 {
			indexer.logger.Warnf("no pruned epoch aggregation found for epoch %v (head: %v)", es.epoch, epochBlocks[0].Root.String())
		}
	}

	// sort blocks ascending
	sort.Slice(epochBlocks, func(i, j int) bool {
		return epochBlocks[i].Slot < epochBlocks[j].Slot
	})

	// compute epoch votes
	epochVotes := es.GetEpochVotes(indexer, headBlock)

	return indexer.dbWriter.buildDbEpoch(es.epoch, epochBlocks, es, epochVotes, nil)
}

// GetEpochVotes aggregates & returns the EpochVotes for the EpochStats.
func (es *EpochStats) GetEpochVotes(indexer *Indexer, headBlock *Block) *EpochVotes {
	chainState := indexer.consensusPool.GetChainState()
	if headBlock == nil {
		headBlock = indexer.GetCanonicalHead(nil)
	}

	// collect all blocks for this & next epoch in chain defined by headBlock
	votingBlocks := []*Block{}
	currentBlock := headBlock
	for {
		if currentBlock == nil || chainState.EpochOfSlot(currentBlock.Slot) < es.epoch {
			break
		}

		if chainState.EpochOfSlot(currentBlock.Slot) == es.epoch || chainState.EpochOfSlot(currentBlock.Slot) == es.epoch+1 {
			votingBlocks = append(votingBlocks, currentBlock)
		}

		parentRoot := currentBlock.GetParentRoot()
		if parentRoot == nil {
			break
		}

		currentBlock = indexer.blockCache.getBlockByRoot(*parentRoot)
	}

	// sort blocks ascending
	sort.Slice(votingBlocks, func(i, j int) bool {
		return votingBlocks[i].Slot < votingBlocks[j].Slot
	})

	// compute epoch votes
	return indexer.aggregateEpochVotes(es.epoch, chainState, votingBlocks, es)
}
