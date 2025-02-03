package dbtypes

type ExplorerState struct {
	Key   string `db:"key"`
	Value string `db:"value"`
}

type ValidatorName struct {
	Index uint64 `db:"index"`
	Name  string `db:"name"`
}

type SlotStatus uint8

const (
	Missing SlotStatus = iota
	Canonical
	Orphaned
)

type SlotHeader struct {
	Slot     uint64     `db:"slot"`
	Proposer uint64     `db:"proposer"`
	Status   SlotStatus `db:"status"`
}

type Slot struct {
	Slot                  uint64     `db:"slot"`
	Proposer              uint64     `db:"proposer"`
	Status                SlotStatus `db:"status"`
	Root                  []byte     `db:"root"`
	ParentRoot            []byte     `db:"parent_root"`
	StateRoot             []byte     `db:"state_root"`
	Graffiti              []byte     `db:"graffiti"`
	GraffitiText          string     `db:"graffiti_text"`
	AttestationCount      uint64     `db:"attestation_count"`
	DepositCount          uint64     `db:"deposit_count"`
	ExitCount             uint64     `db:"exit_count"`
	WithdrawCount         uint64     `db:"withdraw_count"`
	WithdrawAmount        uint64     `db:"withdraw_amount"`
	AttesterSlashingCount uint64     `db:"attester_slashing_count"`
	ProposerSlashingCount uint64     `db:"proposer_slashing_count"`
	BLSChangeCount        uint64     `db:"bls_change_count"`
	EthTransactionCount   uint64     `db:"eth_transaction_count"`
	EthBlockNumber        *uint64    `db:"eth_block_number"`
	EthBlockHash          []byte     `db:"eth_block_hash"`
	EthBlockExtra         []byte     `db:"eth_block_extra"`
	EthBlockExtraText     string     `db:"eth_block_extra_text"`
	SyncParticipation     float32    `db:"sync_participation"`
	ForkId                uint64     `db:"fork_id"`
}

type Epoch struct {
	Epoch                 uint64  `db:"epoch"`
	ValidatorCount        uint64  `db:"validator_count"`
	ValidatorBalance      uint64  `db:"validator_balance"`
	Eligible              uint64  `db:"eligible"`
	VotedTarget           uint64  `db:"voted_target"`
	VotedHead             uint64  `db:"voted_head"`
	VotedTotal            uint64  `db:"voted_total"`
	BlockCount            uint16  `db:"block_count"`
	OrphanedCount         uint16  `db:"orphaned_count"`
	AttestationCount      uint64  `db:"attestation_count"`
	DepositCount          uint64  `db:"deposit_count"`
	ExitCount             uint64  `db:"exit_count"`
	WithdrawCount         uint64  `db:"withdraw_count"`
	WithdrawAmount        uint64  `db:"withdraw_amount"`
	AttesterSlashingCount uint64  `db:"attester_slashing_count"`
	ProposerSlashingCount uint64  `db:"proposer_slashing_count"`
	BLSChangeCount        uint64  `db:"bls_change_count"`
	EthTransactionCount   uint64  `db:"eth_transaction_count"`
	SyncParticipation     float32 `db:"sync_participation"`
}

type OrphanedBlock struct {
	Root      []byte `db:"root"`
	HeaderVer uint64 `db:"header_ver"`
	HeaderSSZ []byte `db:"header_ssz"`
	BlockVer  uint64 `db:"block_ver"`
	BlockSSZ  []byte `db:"block_ssz"`
}

type SlotAssignment struct {
	Slot     uint64 `db:"slot"`
	Proposer uint64 `db:"proposer"`
}

type SyncAssignment struct {
	Period    uint64 `db:"period"`
	Index     uint32 `db:"index"`
	Validator uint64 `db:"validator"`
}

type UnfinalizedBlockStatus uint32

const (
	UnfinalizedBlockStatusUnprocessed UnfinalizedBlockStatus = iota
	UnfinalizedBlockStatusPruned
	UnfinalizedBlockStatusProcessed
)

type UnfinalizedBlock struct {
	Root      []byte                 `db:"root"`
	Slot      uint64                 `db:"slot"`
	HeaderVer uint64                 `db:"header_ver"`
	HeaderSSZ []byte                 `db:"header_ssz"`
	BlockVer  uint64                 `db:"block_ver"`
	BlockSSZ  []byte                 `db:"block_ssz"`
	Status    UnfinalizedBlockStatus `db:"status"`
	ForkId    uint64                 `db:"fork_id"`
}

type UnfinalizedEpoch struct {
	Epoch                 uint64  `db:"epoch"`
	DependentRoot         []byte  `db:"dependent_root"`
	EpochHeadRoot         []byte  `db:"epoch_head_root"`
	EpochHeadForkId       uint64  `db:"epoch_head_fork_id"`
	ValidatorCount        uint64  `db:"validator_count"`
	ValidatorBalance      uint64  `db:"validator_balance"`
	Eligible              uint64  `db:"eligible"`
	VotedTarget           uint64  `db:"voted_target"`
	VotedHead             uint64  `db:"voted_head"`
	VotedTotal            uint64  `db:"voted_total"`
	BlockCount            uint16  `db:"block_count"`
	OrphanedCount         uint16  `db:"orphaned_count"`
	AttestationCount      uint64  `db:"attestation_count"`
	DepositCount          uint64  `db:"deposit_count"`
	ExitCount             uint64  `db:"exit_count"`
	WithdrawCount         uint64  `db:"withdraw_count"`
	WithdrawAmount        uint64  `db:"withdraw_amount"`
	AttesterSlashingCount uint64  `db:"attester_slashing_count"`
	ProposerSlashingCount uint64  `db:"proposer_slashing_count"`
	BLSChangeCount        uint64  `db:"bls_change_count"`
	EthTransactionCount   uint64  `db:"eth_transaction_count"`
	SyncParticipation     float32 `db:"sync_participation"`
}

type Fork struct {
	ForkId     uint64 `db:"fork_id"`
	BaseSlot   uint64 `db:"base_slot"`
	BaseRoot   []byte `db:"base_root"`
	LeafSlot   uint64 `db:"leaf_slot"`
	LeafRoot   []byte `db:"leaf_root"`
	ParentFork uint64 `db:"parent_fork"`
}

type UnfinalizedDuty struct {
	Epoch         uint64 `db:"epoch"`
	DependentRoot []byte `db:"dependent_root"`
	DutiesSSZ     []byte `db:"duties"`
}

type Blob struct {
	Commitment []byte  `db:"commitment"`
	Proof      []byte  `db:"proof"`
	Size       uint32  `db:"size"`
	Blob       *[]byte `db:"blob"`
}

type BlobAssignment struct {
	Root       []byte `db:"root"`
	Commitment []byte `db:"commitment"`
	Slot       uint64 `db:"slot"`
}

type TxFunctionSignature struct {
	Signature string `db:"signature"`
	Bytes     []byte `db:"bytes"`
	Name      string `db:"name"`
}

type TxUnknownFunctionSignature struct {
	Bytes     []byte `db:"bytes"`
	LastCheck uint64 `db:"lastcheck"`
}

type TxPendingFunctionSignature struct {
	Bytes     []byte `db:"bytes"`
	QueueTime uint64 `db:"queuetime"`
}

type MevBlock struct {
	SlotNumber     uint64 `db:"slot_number"`
	BlockHash      []byte `db:"block_hash"`
	BlockNumber    uint64 `db:"block_number"`
	BuilderPubkey  []byte `db:"builder_pubkey"`
	ProposerIndex  uint64 `db:"proposer_index"`
	Proposed       uint8  `db:"proposed"`
	SeenbyRelays   uint64 `db:"seenby_relays"`
	FeeRecipient   []byte `db:"fee_recipient"`
	TxCount        uint64 `db:"tx_count"`
	GasUsed        uint64 `db:"gas_used"`
	BlockValue     []byte `db:"block_value"`
	BlockValueGwei uint64 `db:"block_value_gwei"`
}

type DepositTx struct {
	Index                 uint64 `db:"deposit_index"`
	BlockNumber           uint64 `db:"block_number"`
	BlockTime             uint64 `db:"block_time"`
	BlockRoot             []byte `db:"block_root"`
	PublicKey             []byte `db:"publickey"`
	WithdrawalCredentials []byte `db:"withdrawalcredentials"`
	Amount                uint64 `db:"amount"`
	Signature             []byte `db:"signature"`
	ValidSignature        bool   `db:"valid_signature"`
	Orphaned              bool   `db:"orphaned"`
	TxHash                []byte `db:"tx_hash"`
	TxSender              []byte `db:"tx_sender"`
	TxTarget              []byte `db:"tx_target"`
	ForkId                uint64 `db:"fork_id"`
}

type Deposit struct {
	Index                 *uint64 `db:"deposit_index"`
	SlotNumber            uint64  `db:"slot_number"`
	SlotIndex             uint64  `db:"slot_index"`
	SlotRoot              []byte  `db:"slot_root"`
	Orphaned              bool    `db:"orphaned"`
	PublicKey             []byte  `db:"publickey"`
	WithdrawalCredentials []byte  `db:"withdrawalcredentials"`
	Amount                uint64  `db:"amount"`
	ForkId                uint64  `db:"fork_id"`
}

type VoluntaryExit struct {
	SlotNumber     uint64 `db:"slot_number"`
	SlotIndex      uint64 `db:"slot_index"`
	SlotRoot       []byte `db:"slot_root"`
	Orphaned       bool   `db:"orphaned"`
	ValidatorIndex uint64 `db:"validator"`
	ForkId         uint64 `db:"fork_id"`
}

type SlashingReason uint8

const (
	UnspecifiedSlashing SlashingReason = iota
	ProposerSlashing
	AttesterSlashing
)

type Slashing struct {
	SlotNumber     uint64         `db:"slot_number"`
	SlotIndex      uint64         `db:"slot_index"`
	SlotRoot       []byte         `db:"slot_root"`
	Orphaned       bool           `db:"orphaned"`
	ValidatorIndex uint64         `db:"validator"`
	SlasherIndex   uint64         `db:"slasher"`
	Reason         SlashingReason `db:"reason"`
	ForkId         uint64         `db:"fork_id"`
}

type ConsolidationRequest struct {
	SlotNumber    uint64  `db:"slot_number"`
	SlotRoot      []byte  `db:"slot_root"`
	SlotIndex     uint64  `db:"slot_index"`
	Orphaned      bool    `db:"orphaned"`
	ForkId        uint64  `db:"fork_id"`
	SourceAddress []byte  `db:"source_address"`
	SourceIndex   *uint64 `db:"source_index"`
	SourcePubkey  []byte  `db:"source_pubkey"`
	TargetIndex   *uint64 `db:"target_index"`
	TargetPubkey  []byte  `db:"target_pubkey"`
	TxHash        []byte  `db:"tx_hash"`
	BlockNumber   uint64  `db:"block_number"`
}

type ConsolidationRequestTx struct {
	BlockNumber   uint64  `db:"block_number"`
	BlockIndex    uint64  `db:"block_index"`
	BlockTime     uint64  `db:"block_time"`
	BlockRoot     []byte  `db:"block_root"`
	ForkId        uint64  `db:"fork_id"`
	SourceAddress []byte  `db:"source_address"`
	SourcePubkey  []byte  `db:"source_pubkey"`
	SourceIndex   *uint64 `db:"source_index"`
	TargetPubkey  []byte  `db:"target_pubkey"`
	TargetIndex   *uint64 `db:"target_index"`
	TxHash        []byte  `db:"tx_hash"`
	TxSender      []byte  `db:"tx_sender"`
	TxTarget      []byte  `db:"tx_target"`
	DequeueBlock  uint64  `db:"dequeue_block"`
}

type WithdrawalRequest struct {
	SlotNumber      uint64  `db:"slot_number"`
	SlotRoot        []byte  `db:"slot_root"`
	SlotIndex       uint64  `db:"slot_index"`
	Orphaned        bool    `db:"orphaned"`
	ForkId          uint64  `db:"fork_id"`
	SourceAddress   []byte  `db:"source_address"`
	ValidatorIndex  *uint64 `db:"validator_index"`
	ValidatorPubkey []byte  `db:"validator_pubkey"`
	Amount          int64   `db:"amount"`
	TxHash          []byte  `db:"tx_hash"`
	BlockNumber     uint64  `db:"block_number"`
}

type WithdrawalRequestTx struct {
	BlockNumber     uint64  `db:"block_number"`
	BlockIndex      uint64  `db:"block_index"`
	BlockTime       uint64  `db:"block_time"`
	BlockRoot       []byte  `db:"block_root"`
	ForkId          uint64  `db:"fork_id"`
	SourceAddress   []byte  `db:"source_address"`
	ValidatorPubkey []byte  `db:"validator_pubkey"`
	ValidatorIndex  *uint64 `db:"validator_index"`
	Amount          int64   `db:"amount"`
	TxHash          []byte  `db:"tx_hash"`
	TxSender        []byte  `db:"tx_sender"`
	TxTarget        []byte  `db:"tx_target"`
	DequeueBlock    uint64  `db:"dequeue_block"`
}

type Validator struct {
	ValidatorIndex             uint64 `db:"validator_index"`
	Pubkey                     []byte `db:"pubkey"`
	WithdrawalCredentials      []byte `db:"withdrawal_credentials"`
	EffectiveBalance           uint64 `db:"effective_balance"`
	Slashed                    bool   `db:"slashed"`
	ActivationEligibilityEpoch int64  `db:"activation_eligibility_epoch"`
	ActivationEpoch            int64  `db:"activation_epoch"`
	ExitEpoch                  int64  `db:"exit_epoch"`
	WithdrawableEpoch          int64  `db:"withdrawable_epoch"`
}
