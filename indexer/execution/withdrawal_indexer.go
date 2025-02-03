package execution

import (
	"fmt"
	"math/big"
	"time"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/dora/db"
	"github.com/ethpandaops/dora/dbtypes"
	"github.com/ethpandaops/dora/indexer/beacon"
	"github.com/ethpandaops/dora/utils"
)

const WithdrawalContractAddr = "0x00000961Ef480Eb55e80D19ad83579A64c007002"

// WithdrawalIndexer is the indexer for the eip-7002 consolidation system contract
type WithdrawalIndexer struct {
	indexerCtx *IndexerCtx
	logger     logrus.FieldLogger
	indexer    *contractIndexer[dbtypes.WithdrawalRequestTx]
	matcher    *transactionMatcher[withdrawalRequestMatch]
}

type withdrawalRequestMatch struct {
	slotRoot  []byte
	slotIndex uint64
	txHash    []byte
}

// NewWithdrawalIndexer creates a new withdrawal contract indexer
func NewWithdrawalIndexer(indexer *IndexerCtx) *WithdrawalIndexer {
	batchSize := utils.Config.ExecutionApi.LogBatchSize
	if batchSize == 0 {
		batchSize = 1000
	}

	wi := &WithdrawalIndexer{
		indexerCtx: indexer,
		logger:     indexer.logger.WithField("indexer", "withdrawals"),
	}

	specs := indexer.chainState.GetSpecs()

	// create contract indexer for the withdrawal contract
	wi.indexer = newContractIndexer(
		indexer,
		indexer.logger.WithField("contract-indexer", "withdrawals"),
		&contractIndexerOptions[dbtypes.WithdrawalRequestTx]{
			stateKey:        "indexer.withdrawalindexer",
			batchSize:       batchSize,
			contractAddress: common.HexToAddress(WithdrawalContractAddr),
			deployBlock:     uint64(utils.Config.ExecutionApi.ElectraDeployBlock),
			dequeueRate:     specs.MaxWithdrawalRequestsPerPayload,

			processFinalTx:  wi.processFinalTx,
			processRecentTx: wi.processRecentTx,
			persistTxs:      wi.persistWithdrawalTxs,
		},
	)

	// create transaction matcher for the withdrawal contract
	wi.matcher = newTransactionMatcher(
		indexer,
		indexer.logger.WithField("contract-matcher", "withdrawals"),
		&transactionMatcherOptions[withdrawalRequestMatch]{
			stateKey:    "indexer.withdrawalmatcher",
			deployBlock: uint64(utils.Config.ExecutionApi.ElectraDeployBlock),
			timeLimit:   2 * time.Second,

			matchBlockRange: wi.matchBlockRange,
			persistMatches:  wi.persistMatches,
		},
	)

	go wi.runWithdrawalIndexerLoop()

	return wi
}

// GetMatcherHeight returns the last processed el block number from the transaction matcher
func (wi *WithdrawalIndexer) GetMatcherHeight() uint64 {
	return wi.matcher.GetMatcherHeight()
}

// runWithdrawalIndexerLoop is the main loop for the withdrawal indexer
func (wi *WithdrawalIndexer) runWithdrawalIndexerLoop() {
	defer utils.HandleSubroutinePanic("WithdrawalIndexer.runWithdrawalIndexerLoop", wi.runWithdrawalIndexerLoop)

	for {
		time.Sleep(30 * time.Second)
		wi.logger.Debugf("run withdrawal indexer logic")

		err := wi.indexer.runContractIndexer()
		if err != nil {
			wi.logger.Errorf("indexer error: %v", err)
		}

		err = wi.matcher.runTransactionMatcher(wi.indexer.state.FinalBlock)
		if err != nil {
			wi.logger.Errorf("matcher error: %v", err)
		}
	}
}

// processFinalTx is the callback for the contract indexer to process final transactions
// it parses the transaction and returns the corresponding withdrawal transaction
func (wi *WithdrawalIndexer) processFinalTx(log *types.Log, tx *types.Transaction, header *types.Header, txFrom common.Address, dequeueBlock uint64) (*dbtypes.WithdrawalRequestTx, error) {
	requestTx := wi.parseRequestLog(log)
	if requestTx == nil {
		return nil, fmt.Errorf("invalid withdrawal log")
	}

	txTo := *tx.To()

	requestTx.BlockTime = header.Time
	requestTx.TxSender = txFrom[:]
	requestTx.TxTarget = txTo[:]
	requestTx.DequeueBlock = dequeueBlock

	return requestTx, nil
}

// processRecentTx is the callback for the contract indexer to process recent transactions
// it parses the transaction and returns the corresponding withdrawal transaction
func (wi *WithdrawalIndexer) processRecentTx(log *types.Log, tx *types.Transaction, header *types.Header, txFrom common.Address, dequeueBlock uint64, fork *forkWithClients) (*dbtypes.WithdrawalRequestTx, error) {
	requestTx := wi.parseRequestLog(log)
	if requestTx == nil {
		return nil, fmt.Errorf("invalid withdrawal log")
	}

	txTo := *tx.To()

	requestTx.BlockTime = header.Time
	requestTx.TxSender = txFrom[:]
	requestTx.TxTarget = txTo[:]
	requestTx.DequeueBlock = dequeueBlock

	clBlock := wi.indexerCtx.beaconIndexer.GetBlocksByExecutionBlockHash(phase0.Hash32(log.BlockHash))
	if len(clBlock) > 0 {
		requestTx.ForkId = uint64(clBlock[0].GetForkId())
	} else {
		requestTx.ForkId = uint64(fork.forkId)
	}

	return requestTx, nil
}

// parseRequestLog parses a withdrawal log and returns the corresponding withdrawal transaction
func (wi *WithdrawalIndexer) parseRequestLog(log *types.Log) *dbtypes.WithdrawalRequestTx {
	// data layout:
	// 0-20: sender address (20 bytes)
	// 20-68: validator pubkey (48 bytes)
	// 68-76: amount (8 bytes)

	if len(log.Data) < 76 {
		wi.logger.Warnf("invalid withdrawal log data length: %v", len(log.Data))
		return nil
	}

	senderAddr := log.Data[:20]
	validatorPubkey := log.Data[20:68]
	amount := big.NewInt(0).SetBytes(log.Data[68:76]).Uint64()

	var validatorIndex *uint64
	if index, found := wi.indexerCtx.beaconIndexer.GetValidatorIndexByPubkey(phase0.BLSPubKey(validatorPubkey)); found {
		indexNum := uint64(index)
		validatorIndex = &indexNum
	}

	requestTx := &dbtypes.WithdrawalRequestTx{
		BlockNumber:     log.BlockNumber,
		BlockIndex:      uint64(log.Index),
		BlockRoot:       log.BlockHash[:],
		SourceAddress:   senderAddr,
		ValidatorPubkey: validatorPubkey,
		ValidatorIndex:  validatorIndex,
		Amount:          db.ConvertUint64ToInt64(amount),
		TxHash:          log.TxHash[:],
	}

	return requestTx
}

// persistWithdrawalTxs is the callback for the contract indexer to persist withdrawal transactions to the database
func (wi *WithdrawalIndexer) persistWithdrawalTxs(tx *sqlx.Tx, requests []*dbtypes.WithdrawalRequestTx) error {
	requestCount := len(requests)
	for requestIdx := 0; requestIdx < requestCount; requestIdx += 500 {
		endIdx := requestIdx + 500
		if endIdx > requestCount {
			endIdx = requestCount
		}

		err := db.InsertWithdrawalRequestTxs(requests[requestIdx:endIdx], tx)
		if err != nil {
			return fmt.Errorf("error while inserting withdrawal txs: %v", err)
		}
	}

	return nil
}

// matchBlockRange is the callback for the transaction matcher to match withdrawal requests with their corresponding transactions
func (wi *WithdrawalIndexer) matchBlockRange(fromBlock uint64, toBlock uint64) ([]*withdrawalRequestMatch, error) {
	requestMatches := []*withdrawalRequestMatch{}

	dequeueWithdrawalTxs := db.GetWithdrawalRequestTxsByDequeueRange(fromBlock, toBlock)
	if len(dequeueWithdrawalTxs) > 0 {
		firstBlock := dequeueWithdrawalTxs[0].DequeueBlock
		lastBlock := dequeueWithdrawalTxs[len(dequeueWithdrawalTxs)-1].DequeueBlock

		for _, withdrawalRequest := range db.GetWithdrawalRequestsByElBlockRange(firstBlock, lastBlock) {
			if len(withdrawalRequest.TxHash) > 0 {
				continue
			}

			parentForkIds := wi.indexerCtx.beaconIndexer.GetParentForkIds(beacon.ForkKey(withdrawalRequest.ForkId))
			isParentFork := func(forkId uint64) bool {
				if forkId == withdrawalRequest.ForkId {
					return true
				}
				for _, parentForkId := range parentForkIds {
					if uint64(parentForkId) == forkId {
						return true
					}
				}
				return false
			}

			matchingTxs := []*dbtypes.WithdrawalRequestTx{}
			for _, tx := range dequeueWithdrawalTxs {
				if tx.DequeueBlock == withdrawalRequest.BlockNumber && isParentFork(tx.ForkId) {
					matchingTxs = append(matchingTxs, tx)
				}
			}

			if len(matchingTxs) == 0 {
				for _, tx := range dequeueWithdrawalTxs {
					if tx.DequeueBlock == withdrawalRequest.BlockNumber {
						matchingTxs = append(matchingTxs, tx)
					}
				}
			}

			if len(matchingTxs) < int(withdrawalRequest.SlotIndex)+1 {
				continue
			}

			txHash := matchingTxs[withdrawalRequest.SlotIndex].TxHash
			wi.logger.Debugf("Matched withdrawal request %d:%v with tx 0x%x", withdrawalRequest.SlotNumber, withdrawalRequest.SlotIndex, txHash)

			requestMatches = append(requestMatches, &withdrawalRequestMatch{
				slotRoot:  withdrawalRequest.SlotRoot,
				slotIndex: withdrawalRequest.SlotIndex,
				txHash:    txHash,
			})
		}
	}

	return requestMatches, nil
}

// persistMatches is the callback for the transaction matcher to persist matches to the database
func (wi *WithdrawalIndexer) persistMatches(tx *sqlx.Tx, matches []*withdrawalRequestMatch) error {
	for _, match := range matches {
		err := db.UpdateWithdrawalRequestTxHash(match.slotRoot, match.slotIndex, match.txHash, tx)
		if err != nil {
			return err
		}
	}

	return nil
}
