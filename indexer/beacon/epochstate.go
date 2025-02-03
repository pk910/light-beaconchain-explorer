package beacon

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/phase0"
)

// epochState represents a beacon state which a epoch status depends on.
type epochState struct {
	slotRoot  phase0.Root
	stateRoot phase0.Root

	loadingCancel  context.CancelFunc
	loadingStatus  uint8
	retryCount     uint64
	readyChanMutex sync.Mutex
	readyChan      chan bool
	highPriority   bool

	validatorBalances []phase0.Gwei
	randaoMixes       []phase0.Root
	depositIndex      uint64
	syncCommittee     []phase0.ValidatorIndex
}

// newEpochState creates a new epochState instance with the root of the state to be loaded.
func newEpochState(slotRoot phase0.Root) *epochState {
	return &epochState{
		slotRoot: slotRoot,
	}
}

// dispose cancels the loading process if it is in progress.
func (s *epochState) dispose() {
	if s.loadingCancel != nil {
		s.loadingCancel()
	}
	s.readyChanMutex.Lock()
	if s.readyChan != nil {
		close(s.readyChan)
		s.readyChan = nil
	}
	s.readyChanMutex.Unlock()
}

func (s *epochState) awaitStateLoaded(ctx context.Context, timeout time.Duration) bool {
	s.readyChanMutex.Lock()
	if s.readyChan == nil && s.loadingStatus != 2 {
		s.readyChan = make(chan bool)
	}
	s.readyChanMutex.Unlock()

	timeoutTime := time.Now().Add(timeout)
	for {
		if s.loadingStatus == 2 {
			return true
		}
		if s.retryCount > 10 {
			return false
		}

		select {
		case <-s.readyChan:
			return true
		case <-time.After(time.Until(timeoutTime)):
			return false
		case <-ctx.Done():
			return false
		case <-time.After(5 * time.Second):
		}
	}
}

// loadState loads the state for the epoch from the client.
func (s *epochState) loadState(ctx context.Context, client *Client, cache *epochCache) (*spec.VersionedBeaconState, error) {
	if s.loadingStatus > 0 {
		return nil, fmt.Errorf("already loading")
	}

	s.loadingStatus = 1
	client.logger.Debugf("loading state for slot %v", s.slotRoot.String())

	ctx, cancel := context.WithTimeout(ctx, beaconStateRequestTimeout+(beaconHeaderRequestTimeout*2))
	s.loadingCancel = cancel
	defer func() {
		s.loadingCancel = nil
		cancel()

		if s.loadingStatus == 1 {
			s.loadingStatus = 0
			s.retryCount++
		}
	}()

	var blockHeader *phase0.SignedBeaconBlockHeader

	block := client.indexer.blockCache.getBlockByRoot(s.slotRoot)
	if block != nil {
		blockHeader = block.AwaitHeader(ctx, beaconHeaderRequestTimeout)
	}

	if blockHeader == nil {
		var err error
		blockHeader, err = LoadBeaconHeader(ctx, client, s.slotRoot)
		if err != nil {
			return nil, err
		}
	}

	s.stateRoot = blockHeader.Message.StateRoot

	resState, err := LoadBeaconState(ctx, client, blockHeader.Message.StateRoot)
	if err != nil {
		return nil, err
	}

	err = s.processState(resState, cache)
	if err != nil {
		return nil, err
	}

	s.readyChanMutex.Lock()
	defer s.readyChanMutex.Unlock()
	if s.readyChan != nil {
		close(s.readyChan)
		s.readyChan = nil
	}

	s.loadingStatus = 2
	return resState, nil
}

// processState processes the state and updates the epochState instance.
// the function extracts and unifies all relevant information from the beacon state, so the full beacon state can be dropped from memory afterwards.
func (s *epochState) processState(state *spec.VersionedBeaconState, cache *epochCache) error {
	validatorList, err := state.Validators()
	if err != nil {
		return fmt.Errorf("error getting validators from state %v: %v", s.slotRoot.String(), err)
	}

	if cache != nil {
		slot, err := state.Slot()
		if err != nil {
			return fmt.Errorf("error getting slot from state %v: %v", s.slotRoot.String(), err)
		}

		cache.indexer.validatorCache.updateValidatorSet(slot, s.slotRoot, validatorList)
	}

	validatorPubkeyMap := make(map[phase0.BLSPubKey]phase0.ValidatorIndex)
	for i, v := range validatorList {
		validatorPubkeyMap[v.PublicKey] = phase0.ValidatorIndex(i)
	}

	validatorBalances, err := state.ValidatorBalances()
	if err != nil {
		return fmt.Errorf("error getting validator balances from state %v: %v", s.slotRoot.String(), err)
	}

	s.validatorBalances = validatorBalances

	randaoMixes, err := getStateRandaoMixes(state)
	if err != nil {
		return fmt.Errorf("error getting randao mixes from state %v: %v", s.slotRoot.String(), err)
	}

	s.randaoMixes = randaoMixes
	s.depositIndex = getStateDepositIndex(state)

	if state.Version >= spec.DataVersionAltair {
		currentSyncCommittee, err := getStateCurrentSyncCommittee(state)
		if err != nil {
			return fmt.Errorf("error getting current sync committee from state %v: %v", s.slotRoot.String(), err)
		}

		syncCommittee := make([]phase0.ValidatorIndex, len(currentSyncCommittee))
		for i, v := range currentSyncCommittee {
			syncCommittee[i] = validatorPubkeyMap[v]
		}
		if cache != nil {
			syncCommittee = cache.getOrUpdateSyncCommittee(syncCommittee)
		}
		s.syncCommittee = syncCommittee
	} else {
		s.syncCommittee = []phase0.ValidatorIndex{}
	}

	return nil
}
