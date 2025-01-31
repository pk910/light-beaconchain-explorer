package beacon

import (
	"bytes"
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	v1 "github.com/attestantio/go-eth2-client/api/v1"
	"github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/ethpandaops/dora/clients/consensus"
	"github.com/ethpandaops/dora/db"
	"github.com/ethpandaops/dora/dbtypes"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

// Client represents a consensus pool client that should be used for indexing beacon blocks.
type Client struct {
	indexer  *Indexer
	index    uint16
	client   *consensus.Client
	logger   logrus.FieldLogger
	indexing bool

	priority       int
	archive        bool
	skipValidators bool

	blockSubscription *consensus.Subscription[*v1.BlockEvent]
	headSubscription  *consensus.Subscription[*v1.HeadEvent]

	headRoot phase0.Root
}

// newClient creates a new indexer client for a given consensus pool client.
func newClient(index uint16, client *consensus.Client, priority int, archive bool, skipValidators bool, indexer *Indexer, logger logrus.FieldLogger) *Client {
	return &Client{
		indexer:  indexer,
		index:    index,
		client:   client,
		logger:   logger,
		indexing: false,

		priority:       priority,
		archive:        archive,
		skipValidators: skipValidators,
	}
}

func (c *Client) getContext() context.Context {
	return c.client.GetContext()
}

func (c *Client) GetClient() *consensus.Client {
	return c.client
}

func (c *Client) GetPriority() int {
	return c.priority
}

func (c *Client) GetIndex() uint16 {
	return c.index
}

// startIndexing starts the indexing process for this client.
// attaches block & head event handlers and starts the event processing subroutine.
func (c *Client) startIndexing() {
	if c.indexing {
		return
	}

	c.indexing = true

	// blocking block subscription with a buffer to ensure no blocks are missed
	c.blockSubscription = c.client.SubscribeBlockEvent(100, true)
	c.headSubscription = c.client.SubscribeHeadEvent(100, true)

	go c.startClientLoop()
}

// startClientLoop starts the client event processing subroutine.
func (c *Client) startClientLoop() {
	defer func() {
		if err := recover(); err != nil {
			c.logger.WithError(err.(error)).Errorf("uncaught panic in indexer.beacon.Client.startClientLoop subroutine: %v, stack: %v", err, string(debug.Stack()))
			time.Sleep(10 * time.Second)

			go c.startClientLoop()
		} else {
			c.indexing = false
		}
	}()

	for {
		err := c.runClientLoop()
		if err != nil {
			c.logger.WithError(err).Warnf("error in indexer.beacon.Client.runClientLoop: %v (retrying in 10 sec)", err)
		}

		time.Sleep(10 * time.Second)
	}
}

func (c *Client) emitBlockLogEntry(slot phase0.Slot, root phase0.Root, source string, isNew bool, forkId ForkKey, processingTimes []time.Duration) {
	chainState := c.client.GetPool().GetChainState()

	processingTimesStr := ""
	if len(processingTimes) > 0 {
		str := strings.Builder{}
		str.WriteString(" (")
		for i, pt := range processingTimes {
			if i > 0 {
				str.WriteString(", ")
			}

			str.WriteString(fmt.Sprintf("%v ms", pt.Milliseconds()))
		}
		str.WriteString(")")

		processingTimesStr = str.String()
	}

	if isNew {
		c.logger.Infof("received block %v:%v [0x%x] %v %v fork: %v", chainState.EpochOfSlot(slot), slot, root[:], source, processingTimesStr, forkId)

		c.indexer.metrics.blockLoadDuration.Observe(float64(processingTimes[0].Milliseconds()))
		c.indexer.metrics.blockProcessDuration.Observe(float64(processingTimes[1].Milliseconds()))
		c.indexer.metrics.blockStoreDuration.Observe(float64(processingTimes[2].Milliseconds()))
	} else {
		c.logger.Debugf("received known block %v:%v [0x%x] %v %v fork: %v", chainState.EpochOfSlot(slot), slot, root[:], source, processingTimesStr, forkId)
	}
}

// runClientLoop runs the client event processing subroutine.
func (c *Client) runClientLoop() error {
	// 1 - load & process head block
	headSlot, headRoot := c.client.GetLastHead()
	if bytes.Equal(headRoot[:], consensus.NullRoot[:]) {
		c.logger.Debugf("no chain head from client, retrying later")
		return nil
	}

	c.headRoot = headRoot

	headBlock, isNew, processingTimes, err := c.processBlock(headSlot, headRoot, nil)
	if err != nil {
		return fmt.Errorf("failed processing head block: %v", err)
	}

	if headBlock == nil {
		return fmt.Errorf("failed loading head block: %v", err)
	}

	c.emitBlockLogEntry(headSlot, headRoot, "head", isNew, headBlock.forkId, processingTimes)

	// 2 - backfill old blocks up to the finalization checkpoint or known in cache
	err = c.indexer.withBackfillTracker(func() error {
		return c.backfillParentBlocks(headBlock)
	})
	if err != nil {
		c.logger.Errorf("failed backfilling slots: %v", err)
	}

	// 3 - listen to block / head events
	for {
		select {
		case <-c.client.GetContext().Done():
			return nil
		case blockEvent := <-c.blockSubscription.Channel():
			err := c.processBlockEvent(blockEvent)
			if err != nil {
				c.logger.Errorf("failed processing block %v (%v): %v", blockEvent.Slot, blockEvent.Block.String(), err)
			}
		case headEvent := <-c.headSubscription.Channel():
			err := c.processHeadEvent(headEvent)
			if err != nil {
				c.logger.Errorf("failed processing head %v (%v): %v", headEvent.Slot, headEvent.Block.String(), err)
			}
		}
	}

}

// processBlockEvent processes a block event from the event stream.
func (c *Client) processBlockEvent(blockEvent *v1.BlockEvent) error {
	if c.client.GetStatus() != consensus.ClientStatusOnline && c.client.GetStatus() != consensus.ClientStatusOptimistic {
		// client is not ready, skip
		return nil
	}

	_, err := c.processStreamBlock(blockEvent.Slot, blockEvent.Block)
	return err
}

// processHeadEvent processes a head event from the event stream.
func (c *Client) processHeadEvent(headEvent *v1.HeadEvent) error {
	if c.client.GetStatus() != consensus.ClientStatusOnline && c.client.GetStatus() != consensus.ClientStatusOptimistic {
		// client is not ready, skip
		return nil
	}

	block, err := c.processStreamBlock(headEvent.Slot, headEvent.Block)
	if err != nil {
		return err
	}

	if bytes.Equal(c.headRoot[:], headEvent.Block[:]) {
		// no head progress?
		return nil
	}

	c.logger.Debugf("head %v -> %v", c.headRoot.String(), block.Root.String())

	// check for chain reorgs
	parentRoot := block.GetParentRoot()
	if parentRoot != nil && !bytes.Equal(c.headRoot[:], (*parentRoot)[:]) {
		// chain reorg!

		// ensure parents of new head
		err := c.backfillParentBlocks(block)
		if err != nil {
			c.logger.Errorf("failed backfilling slots after reorg: %v", err)
		}

		// find parent of both heads
		oldBlock := c.indexer.blockCache.getBlockByRoot(c.headRoot)
		if oldBlock == nil {
			c.logger.Warnf("can't find old block after reorg.")
		} else if err := c.processReorg(oldBlock, block); err != nil {
			c.logger.Errorf("failed processing reorg: %v", err)
		}
	}

	chainState := c.client.GetPool().GetChainState()
	dependentRoot := headEvent.CurrentDutyDependentRoot

	var dependentBlock *Block
	if !bytes.Equal(dependentRoot[:], consensus.NullRoot[:]) {
		block.dependentRoot = &dependentRoot

		dependentBlock = c.indexer.blockCache.getBlockByRoot(dependentRoot)
		if dependentBlock == nil {
			c.logger.Warnf("dependent block (%v) not found after backfilling", dependentRoot.String())
		}
	} else {
		dependentBlock = c.indexer.blockCache.getDependentBlock(chainState, block, c)
	}

	// walk back the chain of epoch stats to ensure we have all duties & epoch specific data for the clients chain
	currentBlock := block
	currentEpoch := chainState.EpochOfSlot(currentBlock.Slot)
	minInMemorySlot := c.indexer.getMinInMemorySlot()
	absoluteMinInMemoryEpoch := c.indexer.getAbsoluteMinInMemoryEpoch()
	for {
		if dependentBlock != nil && currentBlock.Slot >= minInMemorySlot {
			epoch := chainState.EpochOfSlot(currentBlock.Slot)

			// only request state for epochs that are allowed in memory by configuration
			// we accept some gaps here, these will be fixed by the pruning/finalization process
			requestState := epoch >= absoluteMinInMemoryEpoch

			// ensure epoch stats for the epoch
			epochStats := c.indexer.epochCache.createOrGetEpochStats(epoch, dependentBlock.Root, requestState)
			if !epochStats.addRequestedBy(c) {
				break
			}
			if epochStats.dependentState == nil && epoch == currentEpoch {
				// always load most recent dependent state to ensure we have the latest validator set
				c.indexer.epochCache.addEpochStateRequest(epochStats)
			}
		} else {
			if dependentBlock == nil {
				c.logger.Debugf("epoch stats check failed: dependent block for %v:%v (%v) not found", currentBlock.Slot, chainState.EpochOfSlot(currentBlock.Slot), currentBlock.Root.String())
			}
			break
		}

		currentBlock = dependentBlock
		dependentBlock = c.indexer.blockCache.getDependentBlock(chainState, currentBlock, c)
	}

	c.headRoot = block.Root
	return nil
}

// processStreamBlock processes a block received from the stream (either via block or head events).
func (c *Client) processStreamBlock(slot phase0.Slot, root phase0.Root) (*Block, error) {
	block, isNew, processingTimes, err := c.processBlock(slot, root, nil)
	if err != nil {
		return nil, err
	}

	c.emitBlockLogEntry(slot, root, "stream", isNew, block.forkId, processingTimes)

	return block, nil
}

// processReorg processes a chain reorganization.
func (c *Client) processReorg(oldHead *Block, newHead *Block) error {
	// find parent of both heads
	reorgBase := oldHead
	forwardDistance := uint64(0)
	rewindDistance := uint64(0)

	for {
		if res, dist := c.indexer.blockCache.getCanonicalDistance(reorgBase.Root, newHead.Root, 0); res {
			forwardDistance = dist
			break
		}

		parentRoot := reorgBase.GetParentRoot()
		if parentRoot == nil {
			reorgBase = nil
			break
		}

		reorgBase = c.indexer.blockCache.getBlockByRoot(*parentRoot)
		if reorgBase == nil {
			break
		}

		rewindDistance++
	}

	if rewindDistance == 0 {
		c.logger.Debugf("chain fast forward! +%v slots (old: %v, new: %v)", forwardDistance, oldHead.Root.String(), newHead.Root.String())
		return nil // just a fast forward
	}
	if forwardDistance == 0 {
		c.logger.Debugf("chain rewind! -%v slots (old: %v, new: %v)", rewindDistance, oldHead.Root.String(), newHead.Root.String())
		return nil // just a rewind
	}

	c.logger.Infof("chain reorg! depth: -%v / +%v (old: %v, new: %v)", rewindDistance, forwardDistance, oldHead.Root.String(), newHead.Root.String())

	// TODO: do something with reorgs?

	return nil
}

// processBlock processes a block (from stream & polling).
func (c *Client) processBlock(slot phase0.Slot, root phase0.Root, header *phase0.SignedBeaconBlockHeader) (block *Block, isNew bool, processingTimes []time.Duration, err error) {
	chainState := c.client.GetPool().GetChainState()
	finalizedSlot := chainState.GetFinalizedSlot()
	processingTimes = make([]time.Duration, 3)

	if slot < finalizedSlot {
		// block is in finalized epoch
		// known block or a new orphaned block

		// don't add to cache, process this block right after loading the details
		block = newBlock(c.indexer.dynSsz, root, slot)

		dbBlockHead := db.GetBlockHeadByRoot(root[:])
		if dbBlockHead != nil {
			block.isInFinalizedDb = true
			block.parentRoot = (*phase0.Root)(dbBlockHead.ParentRoot)
		}

	} else {
		block, _ = c.indexer.blockCache.createOrGetBlock(root, slot)
	}

	err = block.EnsureHeader(func() (*phase0.SignedBeaconBlockHeader, error) {
		if header != nil {
			return header, nil
		}

		t1 := time.Now()
		defer func() {
			processingTimes[0] += time.Since(t1)
		}()

		return LoadBeaconHeader(c.getContext(), c, root)
	})
	if err != nil {
		return
	}

	isNew, err = block.EnsureBlock(func() (*spec.VersionedSignedBeaconBlock, error) {

		t1 := time.Now()
		defer func() {
			processingTimes[0] += time.Since(t1)
		}()

		return LoadBeaconBlock(c.getContext(), c, root)
	})
	if err != nil {
		return
	}

	if slot >= finalizedSlot && isNew {
		c.indexer.blockCache.addBlockToParentMap(block)
		c.indexer.blockCache.addBlockToExecBlockMap(block)
		t1 := time.Now()

		// fork detection
		err2 := c.indexer.forkCache.processBlock(block)
		if err2 != nil {
			c.logger.Warnf("failed processing new fork: %v", err2)
		}

		// insert into unfinalized blocks
		var dbBlock *dbtypes.UnfinalizedBlock
		dbBlock, err = block.buildUnfinalizedBlock(c.indexer.blockCompression)
		if err != nil {
			return
		}

		processingTimes[1] = time.Since(t1)
		t1 = time.Now()

		// write to db
		err = db.RunDBTransaction(func(tx *sqlx.Tx) error {
			err := db.InsertUnfinalizedBlock(dbBlock, tx)
			if err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			return
		}

		processingTimes[2] = time.Since(t1)

		block.isInUnfinalizedDb = true
		c.indexer.blockCache.latestBlock = block
	}

	if slot < finalizedSlot && !block.isInFinalizedDb {
		// process new orphaned block in finalized epoch
		// TODO: insert new orphaned block to db

		c.logger.Errorf("new orphaned block in finalized epoch %v: %v [%v] - OPEN TODO", chainState.EpochOfSlot(slot), slot, root.String())
	}

	return
}

// backfillParentBlocks backfills parent blocks up to the finalization checkpoint or known in cache.
func (c *Client) backfillParentBlocks(headBlock *Block) error {
	chainState := c.client.GetPool().GetChainState()

	// walk backwards and load all blocks until we reach a block that is marked as seen by this client or is smaller than finalized
	parentRoot := *headBlock.GetParentRoot()
	for {
		var parentHead *phase0.SignedBeaconBlockHeader
		parentBlock := c.indexer.blockCache.getBlockByRoot(parentRoot)
		if parentBlock != nil {
			parentBlock.seenMutex.RLock()
			isSeen := parentBlock.seenMap[c.index] != nil
			parentBlock.seenMutex.RUnlock()

			if isSeen {
				break
			}

			parentHead = parentBlock.GetHeader()
		}

		if parentHead == nil {
			headerRsp, err := LoadBeaconHeader(c.getContext(), c, parentRoot)
			if err != nil {
				return fmt.Errorf("could not load parent header [0x%x]: %v", parentRoot, err)
			}
			if headerRsp == nil {
				return fmt.Errorf("could not find parent header [0x%x]", parentRoot)
			}

			parentHead = headerRsp
		}

		parentSlot := parentHead.Message.Slot
		isNewBlock := false

		if parentSlot < chainState.GetFinalizedSlot() {
			c.logger.Debugf("backfill cache: reached finalized slot %v:%v [0x%x]", chainState.EpochOfSlot(parentSlot), parentSlot, parentRoot)
			break
		}

		var processingTimes []time.Duration
		if parentBlock == nil {
			var err error

			parentBlock, isNewBlock, processingTimes, err = c.processBlock(parentSlot, parentRoot, parentHead)
			if err != nil {
				return fmt.Errorf("could not process block [0x%x]: %v", parentRoot, err)
			}
		}

		c.emitBlockLogEntry(parentSlot, parentRoot, "backfill", isNewBlock, parentBlock.forkId, processingTimes)

		if parentSlot == 0 {
			c.logger.Debugf("backfill cache: reached gensis slot [0x%x]", parentRoot)
			break
		}

		parentRootPtr := parentBlock.GetParentRoot()
		if parentRootPtr != nil {
			parentRoot = *parentRootPtr
		} else {
			parentRoot = parentHead.Message.ParentRoot
		}

		if bytes.Equal(parentRoot[:], consensus.NullRoot[:]) {
			c.logger.Infof("backfill cache: reached null root (genesis)")
			break
		}
	}
	return nil
}
