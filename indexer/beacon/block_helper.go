package beacon

import (
	"errors"
	"fmt"

	"github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/altair"
	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/attestantio/go-eth2-client/spec/deneb"
	"github.com/attestantio/go-eth2-client/spec/eip7732"
	"github.com/attestantio/go-eth2-client/spec/electra"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/ethpandaops/dora/utils"
	dynssz "github.com/pk910/dynamic-ssz"
)

var jsonVersionFlag uint64 = 0x40000000
var compressionFlag uint64 = 0x20000000

// marshalVersionedSignedBeaconBlockSSZ marshals a versioned signed beacon block using SSZ encoding.
func marshalVersionedSignedBeaconBlockSSZ(dynSsz *dynssz.DynSsz, block *spec.VersionedSignedBeaconBlock, compress bool) (version uint64, ssz []byte, err error) {
	if utils.Config.KillSwitch.DisableSSZEncoding {
		// SSZ encoding disabled, use json instead
		version, ssz, err = marshalVersionedSignedBeaconBlockJson(block)
	} else {
		// SSZ encoding
		switch block.Version {
		case spec.DataVersionPhase0:
			version = uint64(block.Version)
			ssz, err = dynSsz.MarshalSSZ(block.Phase0)
		case spec.DataVersionAltair:
			version = uint64(block.Version)
			ssz, err = dynSsz.MarshalSSZ(block.Altair)
		case spec.DataVersionBellatrix:
			version = uint64(block.Version)
			ssz, err = dynSsz.MarshalSSZ(block.Bellatrix)
		case spec.DataVersionCapella:
			version = uint64(block.Version)
			ssz, err = dynSsz.MarshalSSZ(block.Capella)
		case spec.DataVersionDeneb:
			version = uint64(block.Version)
			ssz, err = dynSsz.MarshalSSZ(block.Deneb)
		case spec.DataVersionElectra:
			version = uint64(block.Version)
			ssz, err = dynSsz.MarshalSSZ(block.Electra)
		case spec.DataVersionEIP7732:
			version = uint64(block.Version)
			ssz, err = dynSsz.MarshalSSZ(block.EIP7732)
		default:
			err = fmt.Errorf("unknown block version")
		}
	}

	if compress {
		ssz = compressBytes(ssz)
		version |= compressionFlag
	}

	return
}

// unmarshalVersionedSignedBeaconBlockSSZ unmarshals a versioned signed beacon block using SSZ encoding.
func unmarshalVersionedSignedBeaconBlockSSZ(dynSsz *dynssz.DynSsz, version uint64, ssz []byte) (*spec.VersionedSignedBeaconBlock, error) {
	if (version & compressionFlag) != 0 {
		// decompress
		if d, err := decompressBytes(ssz); err != nil {
			return nil, fmt.Errorf("failed to decompress: %v", err)
		} else {
			ssz = d
			version &= ^compressionFlag
		}
	}

	if (version & jsonVersionFlag) != 0 {
		// JSON encoding
		return unmarshalVersionedSignedBeaconBlockJson(version, ssz)
	}

	// SSZ encoding
	block := &spec.VersionedSignedBeaconBlock{
		Version: spec.DataVersion(version),
	}

	switch block.Version {
	case spec.DataVersionPhase0:
		block.Phase0 = &phase0.SignedBeaconBlock{}
		if err := dynSsz.UnmarshalSSZ(block.Phase0, ssz); err != nil {
			return nil, fmt.Errorf("failed to decode phase0 signed beacon block: %v", err)
		}
	case spec.DataVersionAltair:
		block.Altair = &altair.SignedBeaconBlock{}
		if err := dynSsz.UnmarshalSSZ(block.Altair, ssz); err != nil {
			return nil, fmt.Errorf("failed to decode altair signed beacon block: %v", err)
		}
	case spec.DataVersionBellatrix:
		block.Bellatrix = &bellatrix.SignedBeaconBlock{}
		if err := dynSsz.UnmarshalSSZ(block.Bellatrix, ssz); err != nil {
			return nil, fmt.Errorf("failed to decode bellatrix signed beacon block: %v", err)
		}
	case spec.DataVersionCapella:
		block.Capella = &capella.SignedBeaconBlock{}
		if err := dynSsz.UnmarshalSSZ(block.Capella, ssz); err != nil {
			return nil, fmt.Errorf("failed to decode capella signed beacon block: %v", err)
		}
	case spec.DataVersionDeneb:
		block.Deneb = &deneb.SignedBeaconBlock{}
		if err := dynSsz.UnmarshalSSZ(block.Deneb, ssz); err != nil {
			return nil, fmt.Errorf("failed to decode deneb signed beacon block: %v", err)
		}
	case spec.DataVersionElectra:
		block.Electra = &electra.SignedBeaconBlock{}
		if err := dynSsz.UnmarshalSSZ(block.Electra, ssz); err != nil {
			return nil, fmt.Errorf("failed to decode electra signed beacon block: %v", err)
		}
	case spec.DataVersionEIP7732:
		block.EIP7732 = &eip7732.SignedBeaconBlock{}
		if err := dynSsz.UnmarshalSSZ(block.EIP7732, ssz); err != nil {
			return nil, fmt.Errorf("failed to decode eip7732 signed beacon block: %v", err)
		}
	default:
		return nil, fmt.Errorf("unknown block version")
	}
	return block, nil
}

// marshalVersionedSignedBeaconBlockJson marshals a versioned signed beacon block using JSON encoding.
func marshalVersionedSignedBeaconBlockJson(block *spec.VersionedSignedBeaconBlock) (version uint64, jsonRes []byte, err error) {
	switch block.Version {
	case spec.DataVersionPhase0:
		version = uint64(block.Version)
		jsonRes, err = block.Phase0.MarshalJSON()
	case spec.DataVersionAltair:
		version = uint64(block.Version)
		jsonRes, err = block.Altair.MarshalJSON()
	case spec.DataVersionBellatrix:
		version = uint64(block.Version)
		jsonRes, err = block.Bellatrix.MarshalJSON()
	case spec.DataVersionCapella:
		version = uint64(block.Version)
		jsonRes, err = block.Capella.MarshalJSON()
	case spec.DataVersionDeneb:
		version = uint64(block.Version)
		jsonRes, err = block.Deneb.MarshalJSON()
	case spec.DataVersionElectra:
		version = uint64(block.Version)
		jsonRes, err = block.Electra.MarshalJSON()
	case spec.DataVersionEIP7732:
		version = uint64(block.Version)
		jsonRes, err = block.EIP7732.MarshalJSON()
	default:
		err = fmt.Errorf("unknown block version")
	}

	version |= jsonVersionFlag

	return
}

// unmarshalVersionedSignedBeaconBlockJson unmarshals a versioned signed beacon block using JSON encoding.
func unmarshalVersionedSignedBeaconBlockJson(version uint64, ssz []byte) (*spec.VersionedSignedBeaconBlock, error) {
	if version&jsonVersionFlag == 0 {
		return nil, fmt.Errorf("no json encoding")
	}
	block := &spec.VersionedSignedBeaconBlock{
		Version: spec.DataVersion(version - jsonVersionFlag),
	}
	switch block.Version {
	case spec.DataVersionPhase0:
		block.Phase0 = &phase0.SignedBeaconBlock{}
		if err := block.Phase0.UnmarshalJSON(ssz); err != nil {
			return nil, fmt.Errorf("failed to decode phase0 signed beacon block: %v", err)
		}
	case spec.DataVersionAltair:
		block.Altair = &altair.SignedBeaconBlock{}
		if err := block.Altair.UnmarshalJSON(ssz); err != nil {
			return nil, fmt.Errorf("failed to decode altair signed beacon block: %v", err)
		}
	case spec.DataVersionBellatrix:
		block.Bellatrix = &bellatrix.SignedBeaconBlock{}
		if err := block.Bellatrix.UnmarshalJSON(ssz); err != nil {
			return nil, fmt.Errorf("failed to decode bellatrix signed beacon block: %v", err)
		}
	case spec.DataVersionCapella:
		block.Capella = &capella.SignedBeaconBlock{}
		if err := block.Capella.UnmarshalJSON(ssz); err != nil {
			return nil, fmt.Errorf("failed to decode capella signed beacon block: %v", err)
		}
	case spec.DataVersionDeneb:
		block.Deneb = &deneb.SignedBeaconBlock{}
		if err := block.Deneb.UnmarshalJSON(ssz); err != nil {
			return nil, fmt.Errorf("failed to decode deneb signed beacon block: %v", err)
		}
	case spec.DataVersionElectra:
		block.Electra = &electra.SignedBeaconBlock{}
		if err := block.Electra.UnmarshalJSON(ssz); err != nil {
			return nil, fmt.Errorf("failed to decode electra signed beacon block: %v", err)
		}
	case spec.DataVersionEIP7732:
		block.EIP7732 = &eip7732.SignedBeaconBlock{}
		if err := block.EIP7732.UnmarshalJSON(ssz); err != nil {
			return nil, fmt.Errorf("failed to decode eip7732 signed beacon block: %v", err)
		}
	default:
		return nil, fmt.Errorf("unknown block version")
	}
	return block, nil
}

// getBlockExecutionExtraData returns the extra data from the execution payload of a versioned signed beacon block.
func getBlockExecutionExtraData(v *spec.VersionedSignedBeaconBlock) ([]byte, error) {
	switch v.Version {
	case spec.DataVersionBellatrix:
		if v.Bellatrix == nil || v.Bellatrix.Message == nil || v.Bellatrix.Message.Body == nil || v.Bellatrix.Message.Body.ExecutionPayload == nil {
			return nil, errors.New("no bellatrix block")
		}

		return v.Bellatrix.Message.Body.ExecutionPayload.ExtraData, nil
	case spec.DataVersionCapella:
		if v.Capella == nil || v.Capella.Message == nil || v.Capella.Message.Body == nil || v.Capella.Message.Body.ExecutionPayload == nil {
			return nil, errors.New("no capella block")
		}

		return v.Capella.Message.Body.ExecutionPayload.ExtraData, nil
	case spec.DataVersionDeneb:
		if v.Deneb == nil || v.Deneb.Message == nil || v.Deneb.Message.Body == nil || v.Deneb.Message.Body.ExecutionPayload == nil {
			return nil, errors.New("no deneb block")
		}

		return v.Deneb.Message.Body.ExecutionPayload.ExtraData, nil
	case spec.DataVersionElectra:
		if v.Electra == nil || v.Electra.Message == nil || v.Electra.Message.Body == nil || v.Electra.Message.Body.ExecutionPayload == nil {
			return nil, errors.New("no electra block")
		}

		return v.Electra.Message.Body.ExecutionPayload.ExtraData, nil
	case spec.DataVersionEIP7732:
		return nil, nil
	default:
		return nil, errors.New("unknown version")
	}
}

// getStateRandaoMixes returns the RANDAO mixes from a versioned beacon state.
func getStateRandaoMixes(v *spec.VersionedBeaconState) ([]phase0.Root, error) {
	switch v.Version {
	case spec.DataVersionPhase0:
		if v.Phase0 == nil || v.Phase0.RANDAOMixes == nil {
			return nil, errors.New("no phase0 block")
		}

		return v.Phase0.RANDAOMixes, nil
	case spec.DataVersionAltair:
		if v.Altair == nil || v.Altair.RANDAOMixes == nil {
			return nil, errors.New("no altair block")
		}

		return v.Altair.RANDAOMixes, nil
	case spec.DataVersionBellatrix:
		if v.Bellatrix == nil || v.Bellatrix.RANDAOMixes == nil {
			return nil, errors.New("no bellatrix block")
		}

		return v.Bellatrix.RANDAOMixes, nil
	case spec.DataVersionCapella:
		if v.Capella == nil || v.Capella.RANDAOMixes == nil {
			return nil, errors.New("no capella block")
		}

		return v.Capella.RANDAOMixes, nil
	case spec.DataVersionDeneb:
		if v.Deneb == nil || v.Deneb.RANDAOMixes == nil {
			return nil, errors.New("no deneb block")
		}

		return v.Deneb.RANDAOMixes, nil
	case spec.DataVersionElectra:
		if v.Electra == nil || v.Electra.RANDAOMixes == nil {
			return nil, errors.New("no electra block")
		}

		return v.Electra.RANDAOMixes, nil
	case spec.DataVersionEIP7732:
		if v.EIP7732 == nil || v.EIP7732.RANDAOMixes == nil {
			return nil, errors.New("no eip7732 block")
		}

		return v.EIP7732.RANDAOMixes, nil
	default:
		return nil, errors.New("unknown version")
	}
}

// getStateDepositIndex returns the deposit index from a versioned beacon state.
func getStateDepositIndex(state *spec.VersionedBeaconState) uint64 {
	switch state.Version {
	case spec.DataVersionPhase0:
		return state.Phase0.ETH1DepositIndex
	case spec.DataVersionAltair:
		return state.Altair.ETH1DepositIndex
	case spec.DataVersionBellatrix:
		return state.Bellatrix.ETH1DepositIndex
	case spec.DataVersionCapella:
		return state.Capella.ETH1DepositIndex
	case spec.DataVersionDeneb:
		return state.Deneb.ETH1DepositIndex
	case spec.DataVersionElectra:
		return state.Electra.ETH1DepositIndex
	case spec.DataVersionEIP7732:
		return state.EIP7732.ETH1DepositIndex
	}
	return 0
}

// getStateCurrentSyncCommittee returns the current sync committee from a versioned beacon state.
func getStateCurrentSyncCommittee(v *spec.VersionedBeaconState) ([]phase0.BLSPubKey, error) {
	switch v.Version {
	case spec.DataVersionPhase0:
		return nil, errors.New("no sync committee in phase0")
	case spec.DataVersionAltair:
		if v.Altair == nil || v.Altair.CurrentSyncCommittee == nil {
			return nil, errors.New("no altair block")
		}

		return v.Altair.CurrentSyncCommittee.Pubkeys, nil
	case spec.DataVersionBellatrix:
		if v.Bellatrix == nil || v.Bellatrix.CurrentSyncCommittee == nil {
			return nil, errors.New("no bellatrix block")
		}

		return v.Bellatrix.CurrentSyncCommittee.Pubkeys, nil
	case spec.DataVersionCapella:
		if v.Capella == nil || v.Capella.CurrentSyncCommittee == nil {
			return nil, errors.New("no capella block")
		}

		return v.Capella.CurrentSyncCommittee.Pubkeys, nil
	case spec.DataVersionDeneb:
		if v.Deneb == nil || v.Deneb.CurrentSyncCommittee == nil {
			return nil, errors.New("no deneb block")
		}

		return v.Deneb.CurrentSyncCommittee.Pubkeys, nil
	case spec.DataVersionElectra:
		if v.Electra == nil || v.Electra.CurrentSyncCommittee == nil {
			return nil, errors.New("no electra block")
		}

		return v.Electra.CurrentSyncCommittee.Pubkeys, nil
	case spec.DataVersionEIP7732:
		if v.EIP7732 == nil || v.EIP7732.CurrentSyncCommittee == nil {
			return nil, errors.New("no eip7732 block")
		}

		return v.EIP7732.CurrentSyncCommittee.Pubkeys, nil
	default:
		return nil, errors.New("unknown version")
	}
}
