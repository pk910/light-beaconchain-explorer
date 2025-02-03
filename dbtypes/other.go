package dbtypes

import v1 "github.com/attestantio/go-eth2-client/api/v1"

type AssignedSlot struct {
	Slot     uint64 `db:"slot"`
	Proposer uint64 `db:"proposer"`
	Block    *Slot  `db:"block"`
}

type BlockStatus struct {
	Root   []byte     `db:"root"`
	Status SlotStatus `db:"status"`
}

type BlockHead struct {
	Slot       uint64 `db:"slot"`
	Root       []byte `db:"root"`
	ParentRoot []byte `db:"parent_root"`
	ForkId     uint64 `db:"fork_id"`
}

type AssignedBlob struct {
	Root       []byte `db:"root"`
	Commitment []byte `db:"commitment"`
	Slot       uint64 `db:"slot"`
	Blob       *Blob  `db:"blob"`
}

type UnfinalizedBlockFilter struct {
	MinSlot  uint64
	MaxSlot  uint64
	WithBody bool
}

type BlockFilter struct {
	Graffiti      string
	ExtraData     string
	ProposerIndex *uint64
	ProposerName  string
	WithOrphaned  uint8
	WithMissing   uint8
}

type MevBlockFilter struct {
	MinSlot       uint64
	MaxSlot       uint64
	MinIndex      uint64
	MaxIndex      uint64
	ProposerName  string
	BuilderPubkey []byte
	Proposed      []uint8
	MevRelay      []uint8
}

type DepositTxFilter struct {
	MinIndex      uint64
	MaxIndex      uint64
	Address       []byte
	TargetAddress []byte
	PublicKey     []byte
	PublicKeys    [][]byte
	ValidatorName string
	MinAmount     uint64
	MaxAmount     uint64
	WithOrphaned  uint8
	WithValid     uint8
}

type DepositFilter struct {
	MinIndex      uint64
	MaxIndex      uint64
	PublicKey     []byte
	ValidatorName string
	MinAmount     uint64
	MaxAmount     uint64
	WithOrphaned  uint8
}

type VoluntaryExitFilter struct {
	MinSlot       uint64
	MaxSlot       uint64
	MinIndex      uint64
	MaxIndex      uint64
	ValidatorName string
	WithOrphaned  uint8
}

type SlashingFilter struct {
	MinSlot       uint64
	MaxSlot       uint64
	MinIndex      uint64
	MaxIndex      uint64
	ValidatorName string
	SlasherName   string
	WithOrphaned  uint8
	WithReason    SlashingReason
}

type WithdrawalRequestFilter struct {
	MinSlot       uint64
	MaxSlot       uint64
	PublicKey     []byte
	SourceAddress []byte
	MinIndex      uint64
	MaxIndex      uint64
	ValidatorName string
	MinAmount     *uint64
	MaxAmount     *uint64
	WithOrphaned  uint8
}

type WithdrawalRequestTxFilter struct {
	MinDequeue    uint64
	MaxDequeue    uint64
	PublicKey     []byte
	SourceAddress []byte
	MinIndex      uint64
	MaxIndex      uint64
	ValidatorName string
	MinAmount     *uint64
	MaxAmount     *uint64
	WithOrphaned  uint8
}

type ConsolidationRequestFilter struct {
	MinSlot          uint64
	MaxSlot          uint64
	PublicKey        []byte
	SourceAddress    []byte
	MinSrcIndex      uint64
	MaxSrcIndex      uint64
	SrcValidatorName string
	MinTgtIndex      uint64
	MaxTgtIndex      uint64
	TgtValidatorName string
	WithOrphaned     uint8
}

type ConsolidationRequestTxFilter struct {
	MinDequeue       uint64
	MaxDequeue       uint64
	PublicKey        []byte
	SourceAddress    []byte
	MinSrcIndex      uint64
	MaxSrcIndex      uint64
	SrcValidatorName string
	MinTgtIndex      uint64
	MaxTgtIndex      uint64
	TgtValidatorName string
	WithOrphaned     uint8
}

type ValidatorOrder uint8

const (
	ValidatorOrderIndexAsc ValidatorOrder = iota
	ValidatorOrderIndexDesc
	ValidatorOrderPubKeyAsc
	ValidatorOrderPubKeyDesc
	ValidatorOrderBalanceAsc
	ValidatorOrderBalanceDesc
	ValidatorOrderActivationEpochAsc
	ValidatorOrderActivationEpochDesc
	ValidatorOrderExitEpochAsc
	ValidatorOrderExitEpochDesc
	ValidatorOrderWithdrawableEpochAsc
	ValidatorOrderWithdrawableEpochDesc
)

type ValidatorFilter struct {
	MinIndex          *uint64
	MaxIndex          *uint64
	PubKey            []byte
	WithdrawalAddress []byte
	ValidatorName     string
	Status            []v1.ValidatorState

	OrderBy ValidatorOrder
	Limit   uint64
	Offset  uint64
}
