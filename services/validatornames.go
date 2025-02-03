package services

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethpandaops/dora/clients/consensus"
	"github.com/ethpandaops/dora/config"
	"github.com/ethpandaops/dora/db"
	"github.com/ethpandaops/dora/dbtypes"
	"github.com/ethpandaops/dora/indexer/beacon"
	"github.com/ethpandaops/dora/utils"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"gopkg.in/yaml.v3"
)

var logger_vn = logrus.StandardLogger().WithField("module", "validator_names")

type ValidatorNames struct {
	beaconIndexer         *beacon.Indexer
	chainState            *consensus.ChainState
	loadingMutex          sync.Mutex
	loading               chan bool
	lastResolvedMapUpdate time.Time
	lastInventoryRefresh  time.Time
	updaterRunning        bool
	namesMutex            sync.RWMutex
	namesByIndex          map[uint64]*validatorNameEntry
	namesByWithdrawal     map[common.Address]*validatorNameEntry
	namesByDepositOrigin  map[common.Address]*validatorNameEntry
	namesByDepositTarget  map[common.Address]*validatorNameEntry
	resolvedNamesByIndex  map[uint64]*validatorNameEntry
}

type validatorNameEntry struct {
	name string
}

func NewValidatorNames(beaconIndexer *beacon.Indexer, chainState *consensus.ChainState) *ValidatorNames {
	validatorNames := &ValidatorNames{
		beaconIndexer: beaconIndexer,
		chainState:    chainState,
	}
	return validatorNames
}

func (vn *ValidatorNames) StartUpdater() {
	if vn.updaterRunning {
		return
	}
	if utils.Config.Frontend.ValidatorNamesRefreshInterval == 0 {
		utils.Config.Frontend.ValidatorNamesRefreshInterval = 2 * time.Hour
	}
	if utils.Config.Frontend.ValidatorNamesResolveInterval == 0 {
		utils.Config.Frontend.ValidatorNamesResolveInterval = 6 * time.Hour
	}

	vn.updaterRunning = true
	go vn.runUpdaterLoop()
}

func (vn *ValidatorNames) runUpdaterLoop() {
	defer utils.HandleSubroutinePanic("ValidatorNames.runUpdaterLoop", vn.runUpdaterLoop)

	for {
		time.Sleep(30 * time.Second)

		err := vn.runUpdater()
		if err != nil {
			logger_vn.Errorf("validator names update error: %v, retrying in 30 sec...", err)
		}
	}
}

func (vn *ValidatorNames) runUpdater() error {
	needUpdate := false

	if utils.Config.Frontend.ValidatorNamesRefreshInterval > 0 && time.Since(vn.lastInventoryRefresh) > utils.Config.Frontend.ValidatorNamesRefreshInterval {
		logger_vn.Infof("refreshing validator inventory")
		loadingChan := vn.LoadValidatorNames()
		<-loadingChan
		needUpdate = true
	}

	if time.Since(vn.lastResolvedMapUpdate) > utils.Config.Frontend.ValidatorNamesResolveInterval {
		changes, err := vn.resolveNames()
		if err != nil {
			return err
		}

		vn.lastResolvedMapUpdate = time.Now()
		if changes {
			needUpdate = true
		}
	}

	if needUpdate {
		err := vn.UpdateDb()
		if err != nil {
			return err
		}
	}

	return nil
}

func (vn *ValidatorNames) getDefaultValidatorNames() string {
	specs := vn.chainState.GetSpecs()
	chainName := ""
	if specs != nil {
		chainName = specs.ConfigName
	}

	// default validator names
	switch chainName {
	case "sepolia":
		return "~internal/sepolia.names.yml"
	case "holesky":
		return "~internal/holesky.names.yml"
	}

	return ""
}

func (vn *ValidatorNames) resolveNames() (bool, error) {
	logger_vn.Debugf("resolve validator names")

	newResolvedNames := map[uint64]*validatorNameEntry{}
	hasUpdates := false
	addResolved := func(index uint64, name *validatorNameEntry) {
		newResolvedNames[index] = name
		if vn.resolvedNamesByIndex[index] != name {
			hasUpdates = true
		}
	}

	// resolve names by withdrawal address
	for wdAddr, name := range vn.namesByWithdrawal {
		if name == nil {
			continue
		}

		validators, _ := GlobalBeaconService.GetFilteredValidatorSet(&dbtypes.ValidatorFilter{
			WithdrawalAddress: wdAddr[:],
		}, false)
		for _, validator := range validators {
			addResolved(uint64(validator.Index), name)
		}
	}

	// resolve names by depositor address
	for address := range vn.namesByDepositOrigin {
		offset := uint64(0)
		pageSize := uint64(5000)

		for {
			deposits, depositCount, _ := db.GetDepositTxsFiltered(offset, uint32(pageSize), 0, &dbtypes.DepositTxFilter{
				Address: address[:],
			})
			for _, deposit := range deposits {
				validatorIndex, found := vn.beaconIndexer.GetValidatorIndexByPubkey(phase0.BLSPubKey(deposit.PublicKey))
				if found {
					addResolved(uint64(validatorIndex), vn.namesByDepositOrigin[address])
				}
			}

			offset += pageSize
			if offset > depositCount {
				break
			}
		}
	}

	// resolve names by deposit target address
	for address := range vn.namesByDepositTarget {
		offset := uint64(0)
		pageSize := uint64(5000)

		for {
			deposits, depositCount, _ := db.GetDepositTxsFiltered(offset, uint32(pageSize), 0, &dbtypes.DepositTxFilter{
				TargetAddress: address[:],
			})
			for _, deposit := range deposits {
				validatorIndex, found := vn.beaconIndexer.GetValidatorIndexByPubkey(phase0.BLSPubKey(deposit.PublicKey))
				if found {
					addResolved(uint64(validatorIndex), vn.namesByDepositTarget[address])
				}
			}

			offset += pageSize
			if offset > depositCount {
				break
			}
		}
	}

	if !hasUpdates {
		// check for removed names
		for index := range vn.resolvedNamesByIndex {
			if newResolvedNames[index] == nil {
				hasUpdates = true
			}
		}
	}

	if hasUpdates {
		vn.resolvedNamesByIndex = newResolvedNames
	}

	return hasUpdates, nil
}

func (vn *ValidatorNames) GetValidatorName(index uint64) string {
	if !vn.namesMutex.TryRLock() {
		return ""
	}
	defer vn.namesMutex.RUnlock()
	if vn.namesByIndex == nil {
		return ""
	}

	name := vn.namesByIndex[index]
	if name != nil {
		return name.name
	}

	name = vn.resolvedNamesByIndex[index]
	if name != nil {
		return name.name
	}

	return ""
}

func (vn *ValidatorNames) GetValidatorNameByPubkey(pubkey []byte) string {
	validatorIndex, found := vn.beaconIndexer.GetValidatorIndexByPubkey(phase0.BLSPubKey(pubkey))
	if !found {
		return ""
	}

	return vn.GetValidatorName(uint64(validatorIndex))
}

func (vn *ValidatorNames) GetValidatorNamesCount() uint64 {
	if !vn.namesMutex.TryRLock() {
		return 0
	}
	defer vn.namesMutex.RUnlock()
	if vn.namesByIndex == nil {
		return 0
	}
	return uint64(len(maps.Keys(vn.namesByIndex)) + len(maps.Keys(vn.namesByWithdrawal)))
}

func (vn *ValidatorNames) LoadValidatorNames() chan bool {
	vn.loadingMutex.Lock()
	defer vn.loadingMutex.Unlock()
	if vn.loading != nil {
		return vn.loading
	}
	vn.loading = make(chan bool)

	go func() {
		defer func() {
			vn.loadingMutex.Lock()
			defer vn.loadingMutex.Unlock()
			close(vn.loading)
			vn.loading = nil
			vn.lastInventoryRefresh = time.Now()
		}()

		vn.namesMutex.Lock()
		vn.namesByIndex = make(map[uint64]*validatorNameEntry)
		vn.namesByWithdrawal = make(map[common.Address]*validatorNameEntry)
		vn.namesByDepositOrigin = make(map[common.Address]*validatorNameEntry)
		vn.namesByDepositTarget = make(map[common.Address]*validatorNameEntry)
		vn.namesMutex.Unlock()

		validatorNamesYaml := utils.Config.Frontend.ValidatorNamesYaml
		if validatorNamesYaml == "" {
			validatorNamesYaml = vn.getDefaultValidatorNames()
		}

		// load names
		if strings.HasPrefix(validatorNamesYaml, "~internal/") {
			err := vn.loadFromInternalYaml(validatorNamesYaml[10:])
			if err != nil {
				logger_vn.WithError(err).Errorf("error while loading validator names from internal yaml")
			}
		} else if validatorNamesYaml != "" {
			err := vn.loadFromYaml(validatorNamesYaml)
			if err != nil {
				logger_vn.WithError(err).Errorf("error while loading validator names from yaml")
			}
		}
		if utils.Config.Frontend.ValidatorNamesInventory != "" {
			err := vn.loadFromRangesApi(utils.Config.Frontend.ValidatorNamesInventory)
			if err != nil {
				logger_vn.WithError(err).Errorf("error while loading validator names inventory")
			}
		}
	}()

	return vn.loading
}

func (vn *ValidatorNames) loadFromYaml(fileName string) error {
	f, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("error opening validator names file %v: %v", fileName, err)
	}
	defer f.Close()

	namesYaml := map[string]string{}
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&namesYaml)
	if err != nil {
		return fmt.Errorf("error decoding validator names file %v: %v", fileName, err)
	}

	nameCount := vn.parseNamesMap(namesYaml)
	logger_vn.Infof("loaded %v validator names from yaml (%v)", nameCount, fileName)

	return nil
}

func (vn *ValidatorNames) loadFromInternalYaml(fileName string) error {
	f, err := config.ValidatorNamesYml.Open(fileName)
	if err != nil {
		return fmt.Errorf("could not find internal validator names file %v: %v", fileName, err)
	}

	namesYaml := map[string]string{}
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&namesYaml)
	if err != nil {
		return fmt.Errorf("could not find internal validator names file %v: %v", fileName, err)
	}

	nameCount := vn.parseNamesMap(namesYaml)
	logger_vn.Infof("loaded %v validator names from internal yaml (%v)", nameCount, fileName)

	return nil
}

func (vn *ValidatorNames) parseNamesMap(names map[string]string) int {
	vn.namesMutex.Lock()
	defer vn.namesMutex.Unlock()
	nameCount := 0
	for idxStr, name := range names {
		rangeParts := strings.Split(idxStr, ":")
		nameEntry := &validatorNameEntry{
			name: name,
		}

		if len(rangeParts) > 1 {
			switch rangeParts[0] {
			case "withdrawal":
				withdrawal := common.HexToAddress(rangeParts[1])
				vn.namesByWithdrawal[withdrawal] = nameEntry
				nameCount++
			case "depositor", "deposit_origin":
				depositor := common.HexToAddress(rangeParts[1])
				vn.namesByDepositOrigin[depositor] = nameEntry
				nameCount++
			case "deposit_target":
				target := common.HexToAddress(rangeParts[1])
				vn.namesByDepositTarget[target] = nameEntry
				nameCount++
			}

		} else {
			rangeParts = strings.Split(idxStr, "-")
			minIdx, err := strconv.ParseUint(rangeParts[0], 10, 64)
			if err != nil {
				continue
			}
			maxIdx := minIdx
			if len(rangeParts) > 1 {
				maxIdx, err = strconv.ParseUint(rangeParts[1], 10, 64)
				if err != nil {
					continue
				}
			}
			for idx := minIdx; idx <= maxIdx; idx++ {
				vn.namesByIndex[idx] = nameEntry
				nameCount++
			}
		}
	}
	return nameCount
}

type validatorNamesRangesResponse struct {
	Ranges map[string]string `json:"ranges"`
}

func (vn *ValidatorNames) loadFromRangesApi(apiUrl string) error {
	logger_vn.Debugf("Loading validator names from inventory: %v", apiUrl)

	client := &http.Client{Timeout: time.Second * 120}
	resp, err := client.Get(apiUrl)
	if err != nil {
		return fmt.Errorf("could not fetch inventory (%v): %v", utils.GetRedactedUrl(apiUrl), err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			logger_vn.Errorf("could not fetch inventory (%v): not found", utils.GetRedactedUrl(apiUrl))
			return nil
		}
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("url: %v, error-response: %s", utils.GetRedactedUrl(apiUrl), data)
	}
	rangesResponse := &validatorNamesRangesResponse{}
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&rangesResponse)
	if err != nil {
		return fmt.Errorf("error parsing validator ranges response: %v", err)
	}

	nameCount := vn.parseNamesMap(rangesResponse.Ranges)
	logger_vn.Infof("loaded %v validator names from inventory api (%v)", nameCount, utils.GetRedactedUrl(apiUrl))
	return nil
}

func (vn *ValidatorNames) UpdateDb() error {
	vn.namesMutex.RLock()
	nameRows := make([]*dbtypes.ValidatorName, 0)
	hasName := map[uint64]bool{}
	for index, name := range vn.namesByIndex {
		hasName[index] = true
		nameRows = append(nameRows, &dbtypes.ValidatorName{
			Index: index,
			Name:  name.name,
		})
	}
	for index, name := range vn.resolvedNamesByIndex {
		if hasName[index] {
			continue
		}
		nameRows = append(nameRows, &dbtypes.ValidatorName{
			Index: index,
			Name:  name.name,
		})
	}
	vn.namesMutex.RUnlock()

	sort.Slice(nameRows, func(a, b int) bool {
		return nameRows[a].Index < nameRows[b].Index
	})

	batchSize := 10000

	lastIndex := uint64(0)
	nameIdx := 0
	nameLen := len(nameRows)
	for nameIdx < nameLen {
		maxIdx := nameIdx + batchSize
		if maxIdx >= nameLen {
			maxIdx = nameLen
		}
		sliceLen := maxIdx - nameIdx
		namesSlice := nameRows[nameIdx:maxIdx]
		maxIndex := namesSlice[sliceLen-1].Index

		// get existing db entries
		dbNamesMap := map[uint64]string{}
		for _, dbName := range db.GetValidatorNames(lastIndex, maxIndex) {
			dbNamesMap[dbName.Index] = dbName.Name
		}

		// get diffs
		updateNames := make([]*dbtypes.ValidatorName, 0)
		for _, nameRow := range namesSlice {
			dbName := dbNamesMap[nameRow.Index]
			delete(dbNamesMap, nameRow.Index)
			if dbName == nameRow.Name {
				continue // no update
			}
			updateNames = append(updateNames, nameRow)
		}

		removeIndexes := make([]uint64, 0)
		for index := range dbNamesMap {
			removeIndexes = append(removeIndexes, index)
		}

		err := db.RunDBTransaction(func(tx *sqlx.Tx) error {
			if len(updateNames) > 0 {
				err := db.InsertValidatorNames(updateNames, tx)
				if err != nil {
					logger_vn.WithError(err).Errorf("error while adding validator names to db")
				}
			}

			if len(removeIndexes) > 0 {
				err := db.DeleteValidatorNames(removeIndexes, tx)
				if err != nil {
					logger_vn.WithError(err).Errorf("error while deleting validator names from db")
				}
			}

			return nil
		})
		if err != nil {
			return err
		}

		if len(updateNames) > 0 || len(removeIndexes) > 0 {
			logger_vn.Infof("update validator names %v-%v: %v changed, %v removed", lastIndex, maxIndex, len(updateNames), len(removeIndexes))
			time.Sleep(2 * time.Second)
		} else {
			time.Sleep(100 * time.Millisecond)
		}

		lastIndex = maxIndex + 1
		nameIdx = maxIdx
	}

	return nil
}
