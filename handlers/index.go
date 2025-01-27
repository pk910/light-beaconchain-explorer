package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	v1 "github.com/attestantio/go-eth2-client/api/v1"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethpandaops/dora/db"
	"github.com/ethpandaops/dora/dbtypes"
	"github.com/ethpandaops/dora/services"
	"github.com/ethpandaops/dora/templates"
	"github.com/ethpandaops/dora/types/models"
	"github.com/ethpandaops/dora/utils"
	"github.com/sirupsen/logrus"
)

// Index will return the main "index" page using a go template
func Index(w http.ResponseWriter, r *http.Request) {
	var indexTemplateFiles = append(layoutTemplateFiles,
		"index/index.html",
		"index/networkOverview.html",
		"index/recentBlocks.html",
		"index/recentEpochs.html",
		"index/recentSlots.html",
		"_svg/timeline.html",
	)

	var indexTemplate = templates.GetTemplate(indexTemplateFiles...)
	data := InitPageData(w, r, "index", "", "", indexTemplateFiles)

	var pageError error
	pageError = services.GlobalCallRateLimiter.CheckCallLimit(r, 1)
	if pageError == nil {
		data.Data, pageError = getIndexPageData()
	}
	if pageError != nil {
		handlePageError(w, r, pageError)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	if handleTemplateError(w, r, "index.go", "Index", "", indexTemplate.ExecuteTemplate(w, "layout", data)) != nil {
		return // an error has occurred and was processed
	}
}

func IndexData(w http.ResponseWriter, r *http.Request) {
	var pageData *models.IndexPageData
	var pageError error
	pageError = services.GlobalCallRateLimiter.CheckCallLimit(r, 1)
	if pageError == nil {
		pageData, pageError = getIndexPageData()
	}
	if pageError != nil {
		handlePageError(w, r, pageError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(pageData)
	if err != nil {
		logrus.WithError(err).Error("error encoding index data")
		http.Error(w, "Internal server error", http.StatusServiceUnavailable)
	}
}

func getIndexPageData() (*models.IndexPageData, error) {
	pageData := &models.IndexPageData{}
	pageCacheKey := "index"
	pageRes, pageErr := services.GlobalFrontendCache.ProcessCachedPage(pageCacheKey, true, pageData, func(pageCall *services.FrontendCacheProcessingPage) interface{} {
		pageData, cacheTimeout := buildIndexPageData()
		pageCall.CacheTimeout = cacheTimeout
		return pageData
	})
	if pageErr == nil && pageRes != nil {
		resData, resOk := pageRes.(*models.IndexPageData)
		if !resOk {
			return nil, ErrInvalidPageModel
		}
		pageData = resData
	}
	return pageData, pageErr
}

func buildIndexPageData() (*models.IndexPageData, time.Duration) {
	logrus.Debugf("index page called")

	recentEpochCount := 7
	recentBlockCount := 7
	recentSlotsCount := 16

	// network overview
	chainState := services.GlobalBeaconService.GetChainState()
	specs := chainState.GetSpecs()
	currentEpoch := chainState.CurrentEpoch()
	currentSlot := chainState.CurrentSlot()
	currentSlotIndex := chainState.SlotToSlotIndex(currentSlot) + 1

	finalizedEpoch, _ := chainState.GetFinalizedCheckpoint()
	justifiedEpoch, _ := chainState.GetJustifiedCheckpoint()

	syncState := dbtypes.IndexerSyncState{}
	db.GetExplorerState("indexer.syncstate", &syncState)
	var isSynced bool
	if finalizedEpoch >= 1 {
		isSynced = syncState.Epoch >= uint64(finalizedEpoch-1)
	} else {
		isSynced = true
	}

	pageData := &models.IndexPageData{
		NetworkName:           specs.ConfigName,
		DepositContract:       common.Address(specs.DepositContractAddress).String(),
		ShowSyncingMessage:    !isSynced,
		SlotsPerEpoch:         specs.SlotsPerEpoch,
		CurrentEpoch:          uint64(currentEpoch),
		CurrentFinalizedEpoch: int64(finalizedEpoch) - 1,
		CurrentJustifiedEpoch: int64(justifiedEpoch) - 1,
		CurrentSlot:           uint64(currentSlot),
		CurrentScheduledCount: specs.SlotsPerEpoch - uint64(currentSlotIndex),
		CurrentEpochProgress:  float64(100) * float64(currentSlotIndex) / float64(specs.SlotsPerEpoch),
	}
	if utils.Config.Chain.DisplayName != "" {
		pageData.NetworkName = utils.Config.Chain.DisplayName
	}

	currentValidatorSet := services.GlobalBeaconService.GetCachedValidatorSet(true)
	if currentValidatorSet != nil {
		for _, validator := range currentValidatorSet {
			if strings.HasPrefix(validator.Status.String(), "active") {
				pageData.ActiveValidatorCount++
				pageData.TotalEligibleEther += uint64(validator.Validator.EffectiveBalance)
				pageData.AverageValidatorBalance += uint64(validator.Balance)
			}
			if validator.Status == v1.ValidatorStatePendingQueued {
				pageData.EnteringValidatorCount++
			}
			if validator.Status == v1.ValidatorStateActiveExiting {
				pageData.ExitingValidatorCount++
			}
		}
		if pageData.AverageValidatorBalance > 0 {
			pageData.AverageValidatorBalance = pageData.AverageValidatorBalance / pageData.ActiveValidatorCount
		}
	}

	pageData.ValidatorsPerEpoch = chainState.GetValidatorChurnLimit(pageData.ActiveValidatorCount)
	pageData.ValidatorsPerDay = pageData.ValidatorsPerEpoch * 225
	depositQueueTime := float64(pageData.EnteringValidatorCount) / float64(pageData.ValidatorsPerDay)
	if depositQueueTime > 0 {
		depositQueueDays, depositQueueFractionalDays := math.Modf(depositQueueTime)
		depositQueueHours := int(depositQueueFractionalDays * 24)
		pageData.NewDepositProcessAfter = fmt.Sprintf("%d days and %d hours", int(depositQueueDays), depositQueueHours)
	}

	networkGenesis, _ := services.GlobalBeaconService.GetGenesis()
	if networkGenesis != nil {
		pageData.GenesisTime = networkGenesis.GenesisTime
		pageData.GenesisForkVersion = networkGenesis.GenesisForkVersion[:]
		pageData.GenesisValidatorsRoot = networkGenesis.GenesisValidatorsRoot[:]
	}

	pageData.NetworkForks = make([]*models.IndexPageDataForks, 0)
	if specs.AltairForkEpoch != nil && *specs.AltairForkEpoch < uint64(18446744073709551615) {
		pageData.NetworkForks = append(pageData.NetworkForks, &models.IndexPageDataForks{
			Name:    "Altair",
			Epoch:   *specs.AltairForkEpoch,
			Version: specs.AltairForkVersion[:],
			Active:  uint64(currentEpoch) >= *specs.AltairForkEpoch,
		})
	}
	if specs.BellatrixForkEpoch != nil && *specs.BellatrixForkEpoch < uint64(18446744073709551615) {
		pageData.NetworkForks = append(pageData.NetworkForks, &models.IndexPageDataForks{
			Name:    "Bellatrix",
			Epoch:   *specs.BellatrixForkEpoch,
			Version: specs.BellatrixForkVersion[:],
			Active:  uint64(currentEpoch) >= *specs.BellatrixForkEpoch,
		})
	}
	if specs.CapellaForkEpoch != nil && *specs.CapellaForkEpoch < uint64(18446744073709551615) {
		pageData.NetworkForks = append(pageData.NetworkForks, &models.IndexPageDataForks{
			Name:    "Capella",
			Epoch:   *specs.CapellaForkEpoch,
			Version: specs.CapellaForkVersion[:],
			Active:  uint64(currentEpoch) >= *specs.CapellaForkEpoch,
		})
	}
	if specs.DenebForkEpoch != nil && *specs.DenebForkEpoch < uint64(18446744073709551615) {
		pageData.NetworkForks = append(pageData.NetworkForks, &models.IndexPageDataForks{
			Name:    "Deneb",
			Epoch:   *specs.DenebForkEpoch,
			Version: specs.DenebForkVersion[:],
			Active:  uint64(currentEpoch) >= *specs.DenebForkEpoch,
		})
	}
	if specs.ElectraForkEpoch != nil && *specs.ElectraForkEpoch < uint64(18446744073709551615) {
		pageData.NetworkForks = append(pageData.NetworkForks, &models.IndexPageDataForks{
			Name:    "Electra",
			Epoch:   *specs.ElectraForkEpoch,
			Version: specs.ElectraForkVersion[:],
			Active:  uint64(currentEpoch) >= *specs.ElectraForkEpoch,
		})
	}
	if specs.FuluForkEpoch != nil && *specs.FuluForkEpoch < uint64(18446744073709551615) {
		pageData.NetworkForks = append(pageData.NetworkForks, &models.IndexPageDataForks{
			Name:    "Fulu",
			Epoch:   *specs.FuluForkEpoch,
			Version: specs.FuluForkVersion[:],
			Active:  uint64(currentEpoch) >= *specs.FuluForkEpoch,
		})
	}

	// load recent epochs
	buildIndexPageRecentEpochsData(pageData, currentEpoch, finalizedEpoch, justifiedEpoch, recentEpochCount)

	// load recent blocks
	buildIndexPageRecentBlocksData(pageData, recentBlockCount)

	// load recent slots
	buildIndexPageRecentSlotsData(pageData, currentSlot, recentSlotsCount)

	return pageData, 12 * time.Second
}

func buildIndexPageRecentEpochsData(pageData *models.IndexPageData, currentEpoch phase0.Epoch, finalizedEpoch phase0.Epoch, justifiedEpoch phase0.Epoch, recentEpochCount int) {
	pageData.RecentEpochs = make([]*models.IndexPageDataEpochs, 0)

	chainState := services.GlobalBeaconService.GetChainState()

	epochsData := services.GlobalBeaconService.GetDbEpochs(uint64(currentEpoch), uint32(recentEpochCount))
	for i := 0; i < len(epochsData); i++ {
		epochData := epochsData[i]
		if epochData == nil {
			continue
		}
		voteParticipation := float64(1)
		if epochData.Eligible > 0 {
			voteParticipation = float64(epochData.VotedTarget) * 100.0 / float64(epochData.Eligible)
		}
		pageData.RecentEpochs = append(pageData.RecentEpochs, &models.IndexPageDataEpochs{
			Epoch:             epochData.Epoch,
			Ts:                chainState.EpochToTime(phase0.Epoch(epochData.Epoch)),
			Finalized:         uint64(finalizedEpoch) > epochData.Epoch,
			Justified:         uint64(justifiedEpoch) > epochData.Epoch,
			EligibleEther:     epochData.Eligible,
			TargetVoted:       epochData.VotedTarget,
			VoteParticipation: voteParticipation,
		})
	}
	pageData.RecentEpochCount = uint64(len(pageData.RecentEpochs))
}

func buildIndexPageRecentBlocksData(pageData *models.IndexPageData, recentBlockCount int) {
	pageData.RecentBlocks = make([]*models.IndexPageDataBlocks, 0)

	chainState := services.GlobalBeaconService.GetChainState()

	blocksData := services.GlobalBeaconService.GetDbBlocksByFilter(&dbtypes.BlockFilter{
		WithOrphaned: 0,
		WithMissing:  0,
	}, 0, uint32(recentBlockCount), 0)
	limit := len(blocksData)
	if limit > recentBlockCount {
		limit = recentBlockCount
	}

	for i := 0; i < limit; i++ {
		blockData := blocksData[i].Block
		if blockData == nil {
			continue
		}
		blockModel := &models.IndexPageDataBlocks{
			Epoch:        uint64(chainState.EpochOfSlot(phase0.Slot(blockData.Slot))),
			Slot:         blockData.Slot,
			Ts:           chainState.SlotToTime(phase0.Slot(blockData.Slot)),
			Proposer:     blockData.Proposer,
			ProposerName: services.GlobalBeaconService.GetValidatorName(blockData.Proposer),
			Status:       uint64(blockData.Status),
			BlockRoot:    blockData.Root,
		}
		if blockData.EthBlockNumber != nil {
			blockModel.WithEthBlock = true
			blockModel.EthBlock = *blockData.EthBlockNumber
			if utils.Config.Frontend.EthExplorerLink != "" {
				blockModel.EthBlockLink, _ = url.JoinPath(utils.Config.Frontend.EthExplorerLink, "block", strconv.FormatUint(blockModel.EthBlock, 10))
			}
		}
		pageData.RecentBlocks = append(pageData.RecentBlocks, blockModel)
	}
	pageData.RecentBlockCount = uint64(len(pageData.RecentBlocks))
}

func buildIndexPageRecentSlotsData(pageData *models.IndexPageData, firstSlot phase0.Slot, slotLimit int) {
	var lastSlot uint64
	if uint64(firstSlot) >= uint64(slotLimit) {
		lastSlot = uint64(firstSlot) - uint64(slotLimit)
	} else {
		lastSlot = 0
	}

	chainState := services.GlobalBeaconService.GetChainState()

	// load slots
	pageData.RecentSlots = make([]*models.IndexPageDataSlots, 0)
	dbSlots := services.GlobalBeaconService.GetDbBlocksForSlots(uint64(firstSlot), uint32(slotLimit), true, true)
	dbIdx := 0
	dbCnt := len(dbSlots)
	blockCount := uint64(0)
	openForks := map[int][]byte{}
	maxOpenFork := 0
	for slotIdx := int64(firstSlot); slotIdx >= int64(lastSlot); slotIdx-- {
		slot := uint64(slotIdx)
		for dbIdx < dbCnt && dbSlots[dbIdx] != nil && dbSlots[dbIdx].Slot == slot {
			dbSlot := dbSlots[dbIdx]
			dbIdx++

			slotData := &models.IndexPageDataSlots{
				Slot:         slot,
				Epoch:        uint64(chainState.EpochOfSlot(phase0.Slot(dbSlot.Slot))),
				Ts:           chainState.SlotToTime(phase0.Slot(slot)),
				Status:       uint64(dbSlot.Status),
				Proposer:     dbSlot.Proposer,
				ProposerName: services.GlobalBeaconService.GetValidatorName(dbSlot.Proposer),
				BlockRoot:    dbSlot.Root,
				ParentRoot:   dbSlot.ParentRoot,
				ForkGraph:    make([]*models.IndexPageDataForkGraph, 0),
			}
			pageData.RecentSlots = append(pageData.RecentSlots, slotData)
			blockCount++
			buildIndexPageSlotGraph(slotData, &maxOpenFork, openForks)

			if blockCount >= uint64(slotLimit) {
				break
			}
		}
	}
	pageData.RecentSlotCount = uint64(blockCount)
	pageData.ForkTreeWidth = (maxOpenFork * 20) + 20
}

func buildIndexPageSlotGraph(slotData *models.IndexPageDataSlots, maxOpenFork *int, openForks map[int][]byte) {
	// fork tree
	var forkGraphIdx int = -1
	var freeForkIdx int = -1
	getForkGraph := func(slotData *models.IndexPageDataSlots, forkIdx int) *models.IndexPageDataForkGraph {
		forkGraph := &models.IndexPageDataForkGraph{}
		graphCount := len(slotData.ForkGraph)
		if graphCount > forkIdx {
			forkGraph = slotData.ForkGraph[forkIdx]
		} else {
			for graphCount <= forkIdx {
				forkGraph = &models.IndexPageDataForkGraph{
					Index: graphCount,
					Left:  10 + (graphCount * 20),
					Tiles: map[string]bool{},
				}
				slotData.ForkGraph = append(slotData.ForkGraph, forkGraph)
				graphCount++
			}
		}
		return forkGraph
	}

	for forkIdx := 0; forkIdx < *maxOpenFork; forkIdx++ {
		forkGraph := getForkGraph(slotData, forkIdx)
		if openForks[forkIdx] == nil {
			if freeForkIdx == -1 {
				freeForkIdx = forkIdx
			}
			continue
		} else {
			forkGraph.Tiles["vline"] = true
			if bytes.Equal(openForks[forkIdx], slotData.BlockRoot) {
				if forkGraphIdx != -1 {
					continue
				}
				forkGraphIdx = forkIdx
				openForks[forkIdx] = slotData.ParentRoot
				forkGraph.Block = true
				for targetIdx := forkIdx + 1; targetIdx < *maxOpenFork; targetIdx++ {
					if openForks[targetIdx] == nil || !bytes.Equal(openForks[targetIdx], slotData.BlockRoot) {
						continue
					}
					for idx := forkIdx + 1; idx <= targetIdx; idx++ {
						splitGraph := getForkGraph(slotData, idx)
						if idx == targetIdx {
							splitGraph.Tiles["tline"] = true
							splitGraph.Tiles["lline"] = true
							splitGraph.Tiles["fork"] = true
						} else {
							splitGraph.Tiles["hline"] = true
						}
					}
					forkGraph.Tiles["rline"] = true
					openForks[targetIdx] = nil
				}
			}
		}
	}
	if forkGraphIdx == -1 && slotData.Status > 0 {
		// fork head
		if freeForkIdx == -1 {
			freeForkIdx = *maxOpenFork
			*maxOpenFork++
		}
		openForks[freeForkIdx] = slotData.ParentRoot
		forkGraph := getForkGraph(slotData, freeForkIdx)
		forkGraph.Block = true
		forkGraph.Tiles["bline"] = true
	}
}
