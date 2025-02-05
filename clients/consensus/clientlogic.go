package consensus

import (
	"bytes"
	"context"
	"fmt"
	"runtime/debug"
	"time"

	v1 "github.com/attestantio/go-eth2-client/api/v1"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/dora/clients/consensus/rpc"
)

func (client *Client) runClientLoop() {
	defer func() {
		if err := recover(); err != nil {
			client.logger.WithError(err.(error)).Errorf("uncaught panic in clients.consensus.Client.runClientLoop subroutine: %v, stack: %v", err, string(debug.Stack()))
			time.Sleep(10 * time.Second)

			go client.runClientLoop()
		}
	}()

	for {
		err := client.checkClient()
		waitTime := 10

		if err == nil {
			err = client.runClientLogic()
		}

		if err == nil {
			client.retryCounter = 0
			return
		}

		client.isOnline = false
		client.lastError = err
		client.lastEvent = time.Now()
		client.retryCounter++

		if client.retryCounter > 10 {
			waitTime = 300
		} else if client.retryCounter > 5 {
			waitTime = 60
		}

		client.logger.Warnf("consensus client error: %v, retrying in %v sec...", err, waitTime)
		time.Sleep(time.Duration(waitTime) * time.Second)
	}
}

func (client *Client) checkClient() error {
	ctx, cancel := context.WithTimeout(client.clientCtx, 60*time.Second)
	defer cancel()

	err := client.rpcClient.Initialize(ctx)
	if err != nil {
		return fmt.Errorf("initialization of attestantio/go-eth2-client failed: %w", err)
	}

	// get node version
	nodeVersion, err := client.rpcClient.GetNodeVersion(ctx)
	if err != nil {
		return fmt.Errorf("error while fetching node version: %v", err)
	}

	client.versionStr = nodeVersion
	client.parseClientVersion(nodeVersion)

	// update node peers
	if err = client.updateNodePeers(ctx); err != nil {
		return fmt.Errorf("could not get node peers for %s: %v", client.endpointConfig.Name, err)
	}

	// get & compare genesis
	genesis, err := client.rpcClient.GetGenesis(ctx)
	if err != nil {
		return fmt.Errorf("error while fetching genesis: %v", err)
	}

	err = client.pool.chainState.setGenesis(genesis)
	if err != nil {
		return fmt.Errorf("invalid genesis: %v", err)
	}

	// get & compare chain specs
	specs, err := client.rpcClient.GetConfigSpecs(ctx)
	if err != nil {
		return fmt.Errorf("error while fetching specs: %v", err)
	}

	warning, err := client.pool.chainState.setClientSpecs(specs)
	if err != nil {
		return fmt.Errorf("invalid chain specs: %v", err)
	}

	if warning != nil {
		client.logger.Warnf("incomplete chain specs: %v", warning)
	}

	// init wallclock
	client.pool.chainState.initWallclock()

	// check synchronization state
	err = client.updateSynchronizationStatus(ctx)
	if err != nil {
		return err
	}

	// check finalization status
	_, err = client.updateFinalityCheckpoints(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (client *Client) runClientLogic() error {
	// get latest header
	err := client.pollClientHead()
	if err != nil {
		return err
	}

	// check latest header / sync status
	if client.isSyncing {
		return fmt.Errorf("beacon node is synchronizing")
	}

	// start event stream
	blockStream := client.rpcClient.NewBlockStream(
		client.clientCtx,
		client.logger,
		rpc.StreamBlockEvent|rpc.StreamHeadEvent|rpc.StreamFinalizedEvent|rpc.StreamExecutionPayloadEvent,
	)
	defer blockStream.Close()

	// process events
	client.lastEvent = time.Now()

	for {
		eventTimeout := time.Since(client.lastEvent)
		if eventTimeout > 30*time.Second {
			eventTimeout = 0
		} else {
			eventTimeout = 30*time.Second - eventTimeout
		}

		select {
		case <-client.clientCtx.Done():
			return nil
		case evt := <-blockStream.EventChan:
			now := time.Now()

			switch evt.Event {
			case rpc.StreamBlockEvent:
				err := client.processBlockEvent(evt.Data.(*v1.BlockEvent))
				if err != nil {
					client.logger.Warnf("failed processing block event: %v", err)
				}

			case rpc.StreamHeadEvent:
				err := client.processHeadEvent(evt.Data.(*v1.HeadEvent))
				if err != nil {
					client.logger.Warnf("failed processing head event: %v", err)
				}

			case rpc.StreamFinalizedEvent:
				err := client.processFinalizedEvent(evt.Data.(*v1.FinalizedCheckpointEvent))
				if err != nil {
					client.logger.Warnf("failed processing finalized event: %v", err)
				}

			case rpc.StreamExecutionPayloadEvent:
				err := client.processExecutionPayloadEvent(evt.Data.(*v1.ExecutionPayloadEvent))
				if err != nil {
					client.logger.Warnf("failed processing execution payload event: %v", err)
				}
			}

			client.logger.Tracef("event (%v) processing time: %v ms", evt.Event, time.Since(now).Milliseconds())
			client.lastEvent = time.Now()
		case streamStatus := <-blockStream.ReadyChan:
			if client.isOnline != streamStatus.Ready {
				client.isOnline = streamStatus.Ready
				if streamStatus.Ready {
					client.logger.Debug("RPC event stream connected")
					client.lastError = nil
				} else {
					client.logger.Debug("RPC event stream disconnected")
					client.lastError = streamStatus.Error
				}
			}
		case <-time.After(eventTimeout):
			client.logger.Debug("no head event since 30 secs, polling chain head")

			err := client.pollClientHead()
			if err != nil {
				client.isOnline = false
				return err
			}

			client.lastEvent = time.Now()
		}

		currentEpoch := client.pool.chainState.CurrentEpoch()
		currentSlot := client.pool.chainState.CurrentSlot()

		if currentEpoch-client.lastSyncUpdateEpoch >= 1 {
			// update sync status
			if err = client.updateSynchronizationStatus(client.clientCtx); err != nil {
				client.isOnline = false
				return fmt.Errorf("could not get synchronization status for %s: %v", client.endpointConfig.Name, err)
			}

			if client.isSyncing {
				return fmt.Errorf("beacon node is synchronizing")
			}
		}

		if currentEpoch-client.lastFinalityUpdateEpoch >= 1 && client.pool.chainState.SlotToSlotIndex(currentSlot) > 1 {
			client.lastFinalityUpdateEpoch = currentEpoch
			go func() {
				// update finality status
				if _, err = client.updateFinalityCheckpoints(client.clientCtx); err != nil {
					client.logger.Errorf("could not get finality checkpoint for %s: %v", client.endpointConfig.Name, err)
				}
			}()
		}

		if currentEpoch-client.lastPeerUpdateEpoch >= 1 {
			client.lastPeerUpdateEpoch = currentEpoch
			go func() {
				// update node peers
				if err = client.updateNodePeers(client.clientCtx); err != nil {
					client.logger.Errorf("could not get node peers for %s: %v", client.endpointConfig.Name, err)
				} else {
					client.logger.WithFields(logrus.Fields{"epoch": currentEpoch, "peers": len(client.peers)}).Debug("updated consensus node peers")
				}
			}()
		}
	}
}

func (client *Client) updateSynchronizationStatus(ctx context.Context) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	syncStatus, err := client.rpcClient.GetNodeSyncing(ctx)
	if err != nil {
		return fmt.Errorf("error while fetching synchronization status: %v", err)
	}

	if syncStatus == nil {
		return fmt.Errorf("could not get synchronization status")
	}

	client.isSyncing = syncStatus.IsSyncing
	client.isOptimistic = syncStatus.IsOptimistic
	client.lastSyncUpdateEpoch = client.pool.chainState.CurrentEpoch()

	return nil
}

func (client *Client) updateNodePeers(ctx context.Context) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var err error
	client.nodeIdentity, err = client.rpcClient.GetNodeIdentity(ctx)
	if err != nil {
		return fmt.Errorf("could not get node peer id: %v", err)
	}

	peers, err := client.rpcClient.GetNodePeers(ctx)
	if err != nil {
		return fmt.Errorf("could not get peers: %v", err)
	}
	client.peers = peers
	client.lastPeerUpdateEpoch = client.pool.chainState.CurrentEpoch()

	return nil
}

func (client *Client) updateFinalityCheckpoints(ctx context.Context) (phase0.Root, error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	finalizedCheckpoints, err := client.rpcClient.GetFinalityCheckpoints(ctx)
	if err != nil {
		return NullRoot, err
	}

	client.lastFinalityUpdateEpoch = client.pool.chainState.CurrentEpoch()

	client.headMutex.Lock()
	if bytes.Equal(client.justifiedRoot[:], finalizedCheckpoints.Justified.Root[:]) {
		client.headMutex.Unlock()
		return finalizedCheckpoints.Finalized.Root, nil
	}

	client.justifiedEpoch = finalizedCheckpoints.Justified.Epoch
	client.justifiedRoot = finalizedCheckpoints.Justified.Root
	client.finalizedEpoch = finalizedCheckpoints.Finalized.Epoch
	client.finalizedRoot = finalizedCheckpoints.Finalized.Root
	client.headMutex.Unlock()

	client.pool.chainState.setFinalizedCheckpoint(finalizedCheckpoints)
	client.checkpointDispatcher.Fire(finalizedCheckpoints)

	return finalizedCheckpoints.Finalized.Root, nil
}

func (client *Client) processBlockEvent(evt *v1.BlockEvent) error {
	client.blockDispatcher.Fire(evt)

	//client.logger.Infof("BLOCK: %v %v", evt.Slot, evt.Block.String())

	return nil
}

func (client *Client) processHeadEvent(evt *v1.HeadEvent) error {
	client.headMutex.Lock()
	client.headSlot = evt.Slot
	client.headRoot = evt.Block
	client.headMutex.Unlock()

	client.headDispatcher.Fire(evt)

	//client.logger.Infof("HEAD: %v %v %v", evt.Slot, evt.Block.String(), evt.EpochTransition)

	return nil
}

func (client *Client) processFinalizedEvent(evt *v1.FinalizedCheckpointEvent) error {
	go func() {
		retry := 0
		for ; retry < 20; retry++ {
			finalizedRoot, err := client.updateFinalityCheckpoints(client.clientCtx)
			if err != nil {
				client.logger.Warnf("error requesting finality checkpoints: %v", err)
			} else if bytes.Equal(finalizedRoot[:], evt.Block[:]) {
				break
			}

			time.Sleep(3 * time.Second)
		}

		client.logger.Debugf("processed finalization_checkpoint event: finalized %v [0x%x], justified %v [0x%x], retry: %v", client.finalizedEpoch, client.finalizedRoot, client.justifiedEpoch, client.justifiedRoot, retry)
	}()

	return nil
}

func (client *Client) pollClientHead() error {
	ctx, cancel := context.WithTimeout(client.clientCtx, 10*time.Second)
	defer cancel()

	latestHeader, err := client.rpcClient.GetLatestBlockHead(ctx)
	if err != nil {
		return fmt.Errorf("could not get latest header: %v", err)
	}

	if latestHeader == nil {
		return fmt.Errorf("could not find latest header")
	}

	client.headMutex.Lock()
	if bytes.Equal(latestHeader.Root[:], client.headRoot[:]) {
		client.headMutex.Unlock()
		return nil
	}

	client.headSlot = latestHeader.Header.Message.Slot
	client.headRoot = latestHeader.Root
	client.headMutex.Unlock()

	client.blockDispatcher.Fire(&v1.BlockEvent{
		Slot:  latestHeader.Header.Message.Slot,
		Block: latestHeader.Root,
	})

	client.headDispatcher.Fire(&v1.HeadEvent{
		Slot:  latestHeader.Header.Message.Slot,
		Block: latestHeader.Root,
		State: latestHeader.Header.Message.StateRoot,
	})

	// update finality checkpoint
	_, err = client.updateFinalityCheckpoints(ctx)
	if err != nil {
		return fmt.Errorf("could not get finality checkpoint: %v", err)
	}

	return nil
}

func (client *Client) processExecutionPayloadEvent(evt *v1.ExecutionPayloadEvent) error {
	client.executionPayloadDispatcher.Fire(evt)

	return nil
}
