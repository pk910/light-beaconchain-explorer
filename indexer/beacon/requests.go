package beacon

import (
	"context"
	"fmt"
	"time"

	"github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/eip7732"
	"github.com/attestantio/go-eth2-client/spec/phase0"
)

// BeaconHeaderRequestTimeout is the timeout duration for beacon header requests.
const beaconHeaderRequestTimeout time.Duration = 30 * time.Second

// BeaconBodyRequestTimeout is the timeout duration for beacon body requests.
const beaconBodyRequestTimeout time.Duration = 30 * time.Second

// BeaconStateRequestTimeout is the timeout duration for beacon state requests.
const beaconStateRequestTimeout time.Duration = 600 * time.Second

// ExecutionPayloadRequestTimeout is the timeout duration for execution payload requests.
const executionPayloadRequestTimeout time.Duration = 30 * time.Second

const beaconStateRetryCount = 10

// LoadBeaconHeader loads the block header from the client.
func LoadBeaconHeader(ctx context.Context, client *Client, root phase0.Root) (*phase0.SignedBeaconBlockHeader, error) {
	ctx, cancel := context.WithTimeout(ctx, beaconHeaderRequestTimeout)
	defer cancel()

	header, err := client.client.GetRPCClient().GetBlockHeaderByBlockroot(ctx, root)
	if err != nil {
		return nil, err
	}

	return header.Header, nil
}

// LoadBeaconHeaderBySlot loads the block header with given slot number from the client.
func LoadBeaconHeaderBySlot(ctx context.Context, client *Client, slot phase0.Slot) (*phase0.SignedBeaconBlockHeader, phase0.Root, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, beaconHeaderRequestTimeout)
	defer cancel()

	header, err := client.client.GetRPCClient().GetBlockHeaderBySlot(ctx, slot)
	if err != nil {
		return nil, phase0.Root{}, false, err
	}

	if header == nil {
		return nil, phase0.Root{}, false, nil
	}

	return header.Header, header.Root, !header.Canonical, nil
}

// LoadBeaconBlock loads the block body from the RPC client.
func LoadBeaconBlock(ctx context.Context, client *Client, root phase0.Root) (*spec.VersionedSignedBeaconBlock, error) {
	ctx, cancel := context.WithTimeout(ctx, beaconBodyRequestTimeout)
	defer cancel()

	body, err := client.client.GetRPCClient().GetBlockBodyByBlockroot(ctx, root)
	if err != nil {
		return nil, err
	}

	return body, nil
}

// LoadBeaconState loads the beacon state from the client.
func LoadBeaconState(ctx context.Context, client *Client, root phase0.Root) (*spec.VersionedBeaconState, error) {
	ctx, cancel := context.WithTimeout(ctx, beaconStateRequestTimeout)
	defer cancel()

	resState, err := client.client.GetRPCClient().GetState(ctx, fmt.Sprintf("0x%x", root[:]))
	if err != nil {
		return nil, err
	}

	return resState, nil
}

// LoadExecutionPayload loads the execution payload from the client.
func LoadExecutionPayload(ctx context.Context, client *Client, root phase0.Root) (*eip7732.SignedExecutionPayloadEnvelope, error) {
	ctx, cancel := context.WithTimeout(ctx, executionPayloadRequestTimeout)
	defer cancel()

	payload, err := client.client.GetRPCClient().GetExecutionPayloadByBlockroot(ctx, root)
	if err != nil {
		return nil, err
	}

	return payload, nil
}
