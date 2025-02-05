package consensus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"gopkg.in/Knetic/govaluate.v3"
)

type ForkVersion struct {
	Epoch           uint64
	CurrentVersion  []byte
	PreviousVersion []byte
}

// https://github.com/ethereum/consensus-specs/blob/dev/configs/mainnet.yaml
type ChainSpec struct {
	PresetBase                         string            `yaml:"PRESET_BASE"`
	ConfigName                         string            `yaml:"CONFIG_NAME" check-if:"false"`
	MinGenesisTime                     time.Time         `yaml:"MIN_GENESIS_TIME"`
	GenesisForkVersion                 phase0.Version    `yaml:"GENESIS_FORK_VERSION"`
	AltairForkVersion                  phase0.Version    `yaml:"ALTAIR_FORK_VERSION"`
	AltairForkEpoch                    *uint64           `yaml:"ALTAIR_FORK_EPOCH"`
	BellatrixForkVersion               phase0.Version    `yaml:"BELLATRIX_FORK_VERSION"`
	BellatrixForkEpoch                 *uint64           `yaml:"BELLATRIX_FORK_EPOCH"`
	CapellaForkVersion                 phase0.Version    `yaml:"CAPELLA_FORK_VERSION"`
	CapellaForkEpoch                   *uint64           `yaml:"CAPELLA_FORK_EPOCH"`
	DenebForkVersion                   phase0.Version    `yaml:"DENEB_FORK_VERSION"`
	DenebForkEpoch                     *uint64           `yaml:"DENEB_FORK_EPOCH"`
	ElectraForkVersion                 phase0.Version    `yaml:"ELECTRA_FORK_VERSION" check-if-fork:"ElectraForkEpoch"`
	ElectraForkEpoch                   *uint64           `yaml:"ELECTRA_FORK_EPOCH"`
	Eip7594ForkVersion                 phase0.Version    `yaml:"EIP7594_FORK_VERSION" check-if-fork:"Eip7594ForkEpoch"`
	Eip7594ForkEpoch                   *uint64           `yaml:"EIP7594_FORK_EPOCH"`
	Eip7732ForkVersion                 phase0.Version    `yaml:"EIP7732_FORK_VERSION" check-if-fork:"Eip7732ForkEpoch"`
	Eip7732ForkEpoch                   *uint64           `yaml:"EIP7732_FORK_EPOCH"`
	SecondsPerSlot                     time.Duration     `yaml:"SECONDS_PER_SLOT"`
	SlotsPerEpoch                      uint64            `yaml:"SLOTS_PER_EPOCH"`
	EpochsPerHistoricalVector          uint64            `yaml:"EPOCHS_PER_HISTORICAL_VECTOR"`
	EpochsPerSlashingVector            uint64            `yaml:"EPOCHS_PER_SLASHINGS_VECTOR"`
	EpochsPerSyncCommitteePeriod       uint64            `yaml:"EPOCHS_PER_SYNC_COMMITTEE_PERIOD"`
	MinSeedLookahead                   uint64            `yaml:"MIN_SEED_LOOKAHEAD"`
	ShuffleRoundCount                  uint64            `yaml:"SHUFFLE_ROUND_COUNT"`
	MaxEffectiveBalance                uint64            `yaml:"MAX_EFFECTIVE_BALANCE"`
	MaxEffectiveBalanceElectra         uint64            `yaml:"MAX_EFFECTIVE_BALANCE_ELECTRA" check-if-fork:"ElectraForkEpoch"`
	TargetCommitteeSize                uint64            `yaml:"TARGET_COMMITTEE_SIZE"`
	MaxCommitteesPerSlot               uint64            `yaml:"MAX_COMMITTEES_PER_SLOT"`
	MinPerEpochChurnLimit              uint64            `yaml:"MIN_PER_EPOCH_CHURN_LIMIT"`
	ChurnLimitQuotient                 uint64            `yaml:"CHURN_LIMIT_QUOTIENT"`
	DomainBeaconProposer               phase0.DomainType `yaml:"DOMAIN_BEACON_PROPOSER"`
	DomainBeaconAttester               phase0.DomainType `yaml:"DOMAIN_BEACON_ATTESTER"`
	DomainSyncCommittee                phase0.DomainType `yaml:"DOMAIN_SYNC_COMMITTEE"`
	SyncCommitteeSize                  uint64            `yaml:"SYNC_COMMITTEE_SIZE"`
	DepositContractAddress             []byte            `yaml:"DEPOSIT_CONTRACT_ADDRESS"`
	MaxConsolidationRequestsPerPayload uint64            `yaml:"MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD" check-if-fork:"ElectraForkEpoch"`
	MaxWithdrawalRequestsPerPayload    uint64            `yaml:"MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD"    check-if-fork:"ElectraForkEpoch"`
	DepositChainId                     uint64            `yaml:"DEPOSIT_CHAIN_ID"`
	MinActivationBalance               uint64            `yaml:"MIN_ACTIVATION_BALANCE"`

	// EIP7594: PeerDAS
	NumberOfColumns              *uint64 `yaml:"NUMBER_OF_COLUMNS"                check-if-fork:"Eip7594ForkEpoch"`
	DataColumnSidecarSubnetCount *uint64 `yaml:"DATA_COLUMN_SIDECAR_SUBNET_COUNT" check-if-fork:"Eip7594ForkEpoch"`
	CustodyRequirement           *uint64 `yaml:"CUSTODY_REQUIREMENT"              check-if-fork:"Eip7594ForkEpoch"`

	// EIP7732: ePBS
	PtcSize                uint64            `yaml:"PTC_SIZE" check-if-fork:"Eip7732ForkEpoch"`
	MaxPayloadAttestations uint64            `yaml:"MAX_PAYLOAD_ATTESTATIONS" check-if-fork:"Eip7732ForkEpoch"`
	DomainPtcAttester      phase0.DomainType `yaml:"DOMAIN_PTC_ATTESTER" check-if-fork:"Eip7732ForkEpoch"`

	// additional dora specific specs
	WhiskForkEpoch *uint64
}

var byteType = reflect.TypeOf(byte(0))
var specExpressionCache = map[string]*govaluate.EvaluableExpression{}
var specExpressionCacheMutex sync.Mutex

func (chain *ChainSpec) CheckMismatch(chain2 *ChainSpec) ([]string, error) {
	mismatches := []string{}

	chainT := reflect.ValueOf(chain).Elem()
	chain2T := reflect.ValueOf(chain2).Elem()

	genericSpecValues := map[string]any{}
	specData, err := json.Marshal(chain)
	if err != nil {
		return nil, fmt.Errorf("error marshalling chain spec: %v", err)
	}
	err = json.Unmarshal(specData, &genericSpecValues)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling chain spec: %v", err)
	}

	for i := 0; i < chainT.NumField(); i++ {
		fieldT := chainT.Type().Field(i)

		// Check both types of conditions
		checkIfExpression := fieldT.Tag.Get("check-if")
		checkIfFork := fieldT.Tag.Get("check-if-fork")

		if checkIfFork != "" {
			checkIfExpression = fmt.Sprintf("(%s ?? 18446744073709551615) < 18446744073709551615", checkIfFork)
		}

		if checkIfExpression != "" {
			ok, err := chain.checkIf(checkIfExpression, genericSpecValues)
			if err != nil {
				return nil, fmt.Errorf("error checking if expression: %v", err)
			}
			if !ok {
				continue
			}
		}

		fieldV := chainT.Field(i)
		field2V := chain2T.Field(i)

		if fieldV.Type().Kind() == reflect.Ptr {
			if !fieldV.IsNil() {
				fieldV = fieldV.Elem()
			}
			if !field2V.IsNil() {
				field2V = field2V.Elem()
			}
		}

		if fieldV.Type().Kind() == reflect.Slice && fieldV.Type().Elem() == byteType {
			// compare byte slices
			bytesA := fieldV.Interface().([]byte)
			bytesB := field2V.Interface().([]byte)

			if !bytes.Equal(bytesA, bytesB) {
				mismatches = append(mismatches, chainT.Type().Field(i).Name)
			}
		} else if fieldV.Interface() != field2V.Interface() {
			if chainT.Field(i).Interface() == reflect.Zero(chainT.Field(i).Type()).Interface() {
				// 0 value on chain side are allowed
				continue
			}
			mismatches = append(mismatches, chainT.Type().Field(i).Name)
		}
	}

	return mismatches, nil
}

func (chain *ChainSpec) checkIf(expressionStr string, genericSpecValues map[string]any) (bool, error) {
	specExpressionCacheMutex.Lock()
	expression, ok := specExpressionCache[expressionStr]
	if !ok {
		var err error
		expression, err = govaluate.NewEvaluableExpression(expressionStr)
		if err != nil {
			specExpressionCacheMutex.Unlock()
			return false, fmt.Errorf("error parsing dynamic spec expression: %v", err)
		}

		specExpressionCache[expressionStr] = expression
	}
	specExpressionCacheMutex.Unlock()

	result, err := expression.Evaluate(genericSpecValues)
	if err != nil {
		return false, fmt.Errorf("error evaluating dynamic spec expression: %v", err)
	}

	value, ok := result.(bool)
	if ok {
		return value, nil
	}

	return false, nil
}

func (chain *ChainSpec) Clone() *ChainSpec {
	res := &ChainSpec{}
	chainT := reflect.ValueOf(chain).Elem()
	chain2T := reflect.ValueOf(res).Elem()

	for i := 0; i < chainT.NumField(); i++ {
		value := chainT.Field(i)
		chain2T.Field(i).Set(value)
	}

	return res
}
