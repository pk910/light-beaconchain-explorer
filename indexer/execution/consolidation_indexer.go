package execution

import (
	"fmt"
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

const ConsolidationContractAddr = "0x0000BBdDc7CE488642fb579F8B00f3a590007251"

// ConsolidationIndexer is the indexer for the eip-7251 consolidation system contract
type ConsolidationIndexer struct {
	indexerCtx *IndexerCtx
	logger     logrus.FieldLogger
	indexer    *contractIndexer[dbtypes.ConsolidationRequestTx]
	matcher    *transactionMatcher[consolidationRequestMatch]
}

type consolidationRequestMatch struct {
	slotRoot  []byte
	slotIndex uint64
	txHash    []byte
}

// NewConsolidationIndexer creates a new consolidation system contract indexer
func NewConsolidationIndexer(indexer *IndexerCtx) *ConsolidationIndexer {
	batchSize := utils.Config.ExecutionApi.LogBatchSize
	if batchSize == 0 {
		batchSize = 1000
	}

	ci := &ConsolidationIndexer{
		indexerCtx: indexer,
		logger:     indexer.logger.WithField("indexer", "consolidations"),
	}

	specs := indexer.chainState.GetSpecs()

	// create contract indexer for the consolidation contract
	ci.indexer = newContractIndexer(
		indexer,
		indexer.logger.WithField("contract-indexer", "consolidations"),
		&contractIndexerOptions[dbtypes.ConsolidationRequestTx]{
			stateKey:        "indexer.consolidationindexer",
			batchSize:       batchSize,
			contractAddress: common.HexToAddress(ConsolidationContractAddr),
			deployBlock:     uint64(utils.Config.ExecutionApi.ElectraDeployBlock),
			dequeueRate:     specs.MaxConsolidationRequestsPerPayload,

			processFinalTx:  ci.processFinalTx,
			processRecentTx: ci.processRecentTx,
			persistTxs:      ci.persistConsolidationTxs,
		},
	)

	// create transaction matcher for the consolidation contract
	ci.matcher = newTransactionMatcher(
		indexer,
		indexer.logger.WithField("contract-matcher", "consolidations"),
		&transactionMatcherOptions[consolidationRequestMatch]{
			stateKey:    "indexer.consolidationmatcher",
			deployBlock: uint64(utils.Config.ExecutionApi.ElectraDeployBlock),
			timeLimit:   2 * time.Second,

			matchBlockRange: ci.matchBlockRange,
			persistMatches:  ci.persistMatches,
		},
	)

	go ci.runConsolidationIndexerLoop()

	return ci
}

// GetMatcherHeight returns the last processed el block number from the transaction matcher
func (ci *ConsolidationIndexer) GetMatcherHeight() uint64 {
	return ci.matcher.GetMatcherHeight()
}

// runConsolidationIndexerLoop is the main loop for the consolidation indexer
func (ci *ConsolidationIndexer) runConsolidationIndexerLoop() {
	defer utils.HandleSubroutinePanic("ConsolidationIndexer.runConsolidationIndexerLoop", ci.runConsolidationIndexerLoop)

	for {
		time.Sleep(30 * time.Second)
		ci.logger.Debugf("run consolidation indexer logic")

		err := ci.indexer.runContractIndexer()
		if err != nil {
			ci.logger.Errorf("indexer error: %v", err)
		}

		err = ci.matcher.runTransactionMatcher(ci.indexer.state.FinalBlock)
		if err != nil {
			ci.logger.Errorf("matcher error: %v", err)
		}
	}
}

// processFinalTx is the callback for the contract indexer for finalized transactions
// it parses the transaction and returns the corresponding consolidation request transaction
func (ci *ConsolidationIndexer) processFinalTx(log *types.Log, tx *types.Transaction, header *types.Header, txFrom common.Address, dequeueBlock uint64) (*dbtypes.ConsolidationRequestTx, error) {
	requestTx := ci.parseRequestLog(log)
	if requestTx == nil {
		return nil, fmt.Errorf("invalid consolidation log")
	}

	txTo := *tx.To()

	requestTx.BlockTime = header.Time
	requestTx.TxSender = txFrom[:]
	requestTx.TxTarget = txTo[:]
	requestTx.DequeueBlock = dequeueBlock

	return requestTx, nil
}

// processRecentTx is the callback for the contract indexer for recent transactions
// it parses the transaction and returns the corresponding consolidation request transaction
func (ci *ConsolidationIndexer) processRecentTx(log *types.Log, tx *types.Transaction, header *types.Header, txFrom common.Address, dequeueBlock uint64, fork *forkWithClients) (*dbtypes.ConsolidationRequestTx, error) {
	requestTx := ci.parseRequestLog(log)
	if requestTx == nil {
		return nil, fmt.Errorf("invalid consolidation log")
	}

	txTo := *tx.To()

	requestTx.BlockTime = header.Time
	requestTx.TxSender = txFrom[:]
	requestTx.TxTarget = txTo[:]
	requestTx.DequeueBlock = dequeueBlock

	clBlock := ci.indexerCtx.beaconIndexer.GetBlocksByExecutionBlockHash(phase0.Hash32(log.BlockHash))
	if len(clBlock) > 0 {
		requestTx.ForkId = uint64(clBlock[0].GetForkId())
	} else {
		requestTx.ForkId = uint64(fork.forkId)
	}

	return requestTx, nil
}

// parseRequestLog parses a consolidation request log and returns the corresponding consolidation request transaction
func (ci *ConsolidationIndexer) parseRequestLog(log *types.Log) *dbtypes.ConsolidationRequestTx {
	// data layout:
	// 0-20: sender address (20 bytes)
	// 20-68: source pubkey (48 bytes)
	// 68-116: target pubkey (48 bytes)

	if len(log.Data) < 116 {
		ci.logger.Warnf("invalid consolidation log data length: %v", len(log.Data))
		return nil
	}

	senderAddr := log.Data[:20]
	sourcePubkey := log.Data[20:68]
	targetPubkey := log.Data[68:116]

	// get the validator indices for the source and target pubkeys
	var sourceIndex, targetIndex *uint64

	if index, found := ci.indexerCtx.beaconIndexer.GetValidatorIndexByPubkey(phase0.BLSPubKey(sourcePubkey)); found {
		indexNum := uint64(index)
		sourceIndex = &indexNum
	}

	if index, found := ci.indexerCtx.beaconIndexer.GetValidatorIndexByPubkey(phase0.BLSPubKey(targetPubkey)); found {
		indexNum := uint64(index)
		targetIndex = &indexNum
	}

	requestTx := &dbtypes.ConsolidationRequestTx{
		BlockNumber:   log.BlockNumber,
		BlockIndex:    uint64(log.Index),
		BlockRoot:     log.BlockHash[:],
		SourceAddress: senderAddr,
		SourcePubkey:  sourcePubkey,
		SourceIndex:   sourceIndex,
		TargetPubkey:  targetPubkey,
		TargetIndex:   targetIndex,
		TxHash:        log.TxHash[:],
	}

	return requestTx
}

// persistConsolidationTxs is the callback for the contract indexer to persist consolidation request transactions to the database
func (ci *ConsolidationIndexer) persistConsolidationTxs(tx *sqlx.Tx, requests []*dbtypes.ConsolidationRequestTx) error {
	requestCount := len(requests)
	for requestIdx := 0; requestIdx < requestCount; requestIdx += 500 {
		endIdx := requestIdx + 500
		if endIdx > requestCount {
			endIdx = requestCount
		}

		err := db.InsertConsolidationRequestTxs(requests[requestIdx:endIdx], tx)
		if err != nil {
			return fmt.Errorf("error while inserting consolidation txs: %v", err)
		}
	}

	return nil
}

// matchBlockRange is the callback for the transaction matcher to match consolidation request transactions to transactions in the database
func (ds *ConsolidationIndexer) matchBlockRange(fromBlock uint64, toBlock uint64) ([]*consolidationRequestMatch, error) {
	requestMatches := []*consolidationRequestMatch{}

	// get all consolidation request transactions that are dequeued in the block range
	dequeueConsolidationTxs := db.GetConsolidationRequestTxsByDequeueRange(fromBlock, toBlock)
	if len(dequeueConsolidationTxs) > 0 {
		firstBlock := dequeueConsolidationTxs[0].DequeueBlock
		lastBlock := dequeueConsolidationTxs[len(dequeueConsolidationTxs)-1].DequeueBlock

		// get all consolidation requests that are in the block range
		for _, consolidationRequest := range db.GetConsolidationRequestsByElBlockRange(firstBlock, lastBlock) {
			if len(consolidationRequest.TxHash) > 0 {
				continue
			}

			// check if the consolidation request is from a parent fork
			parentForkIds := ds.indexerCtx.beaconIndexer.GetParentForkIds(beacon.ForkKey(consolidationRequest.ForkId))
			isParentFork := func(forkId uint64) bool {
				if forkId == consolidationRequest.ForkId {
					return true
				}
				for _, parentForkId := range parentForkIds {
					if uint64(parentForkId) == forkId {
						return true
					}
				}
				return false
			}

			// get all consolidation request transactions that are from the current fork
			matchingTxs := []*dbtypes.ConsolidationRequestTx{}
			for _, tx := range dequeueConsolidationTxs {
				if tx.DequeueBlock == consolidationRequest.BlockNumber && isParentFork(tx.ForkId) {
					matchingTxs = append(matchingTxs, tx)
				}
			}

			// if no consolidation request transactions are from the current fork, get all transactions that dequeued in the requests block number
			if len(matchingTxs) == 0 {
				for _, tx := range dequeueConsolidationTxs {
					if tx.DequeueBlock == consolidationRequest.BlockNumber {
						matchingTxs = append(matchingTxs, tx)
					}
				}
			}

			if len(matchingTxs) < int(consolidationRequest.SlotIndex)+1 {
				continue // no transaction found for this consolidation request
			}

			txHash := matchingTxs[consolidationRequest.SlotIndex].TxHash
			ds.logger.Debugf("Matched consolidation request %d:%v with tx 0x%x", consolidationRequest.SlotNumber, consolidationRequest.SlotIndex, txHash)

			requestMatches = append(requestMatches, &consolidationRequestMatch{
				slotRoot:  consolidationRequest.SlotRoot,
				slotIndex: consolidationRequest.SlotIndex,
				txHash:    txHash,
			})
		}
	}

	return requestMatches, nil
}

// persistMatches is the callback for the transaction matcher to persist matches to the database
func (ds *ConsolidationIndexer) persistMatches(tx *sqlx.Tx, matches []*consolidationRequestMatch) error {
	for _, match := range matches {
		err := db.UpdateConsolidationRequestTxHash(match.slotRoot, match.slotIndex, match.txHash, tx)
		if err != nil {
			return err
		}
	}

	return nil
}
