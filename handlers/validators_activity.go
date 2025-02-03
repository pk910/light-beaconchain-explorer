package handlers

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/ethpandaops/dora/indexer/beacon"
	"github.com/ethpandaops/dora/services"
	"github.com/ethpandaops/dora/templates"
	"github.com/ethpandaops/dora/types/models"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
)

// ValidatorsActivity will return the filtered "slots" page using a go template
func ValidatorsActivity(w http.ResponseWriter, r *http.Request) {
	var pageTemplateFiles = append(layoutTemplateFiles,
		"validators_activity/validators_activity.html",
		"_svg/professor.html",
	)

	var pageTemplate = templates.GetTemplate(pageTemplateFiles...)
	data := InitPageData(w, r, "validators", "/validators/activity", "Validators Activity", pageTemplateFiles)

	urlArgs := r.URL.Query()
	var pageSize uint64 = 50
	if urlArgs.Has("c") {
		pageSize, _ = strconv.ParseUint(urlArgs.Get("c"), 10, 64)
	}
	var pageIdx uint64 = 0
	if urlArgs.Has("s") {
		pageIdx, _ = strconv.ParseUint(urlArgs.Get("s"), 10, 64)
	}

	var sortOrder string
	if urlArgs.Has("o") {
		sortOrder = urlArgs.Get("o")
	}
	if sortOrder == "" {
		sortOrder = "group"
	}

	var groupBy uint64
	if urlArgs.Has("group") {
		groupBy, _ = strconv.ParseUint(urlArgs.Get("group"), 10, 64)
	}
	if groupBy == 0 {
		if services.GlobalBeaconService.GetValidatorNamesCount() > 0 {
			groupBy = 3
		} else {
			groupBy = 1
		}
	}

	var pageError error
	pageError = services.GlobalCallRateLimiter.CheckCallLimit(r, 2)
	if pageError == nil {
		data.Data, pageError = getValidatorsActivityPageData(pageIdx, pageSize, sortOrder, groupBy)
	}
	if pageError != nil {
		handlePageError(w, r, pageError)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	if handleTemplateError(w, r, "slots_filtered.go", "SlotsFiltered", "", pageTemplate.ExecuteTemplate(w, "layout", data)) != nil {
		return // an error has occurred and was processed
	}
}

func getValidatorsActivityPageData(pageIdx uint64, pageSize uint64, sortOrder string, groupBy uint64) (*models.ValidatorsActivityPageData, error) {
	pageData := &models.ValidatorsActivityPageData{}
	pageCacheKey := fmt.Sprintf("validators_activiy:%v:%v:%v:%v", pageIdx, pageSize, sortOrder, groupBy)
	pageRes, pageErr := services.GlobalFrontendCache.ProcessCachedPage(pageCacheKey, true, pageData, func(processingPage *services.FrontendCacheProcessingPage) interface{} {
		processingPage.CacheTimeout = 10 * time.Second
		return buildValidatorsActivityPageData(pageIdx, pageSize, sortOrder, groupBy)
	})
	if pageErr == nil && pageRes != nil {
		resData, resOk := pageRes.(*models.ValidatorsActivityPageData)
		if !resOk {
			return nil, ErrInvalidPageModel
		}
		pageData = resData
	}
	return pageData, pageErr
}

func buildValidatorsActivityPageData(pageIdx uint64, pageSize uint64, sortOrder string, groupBy uint64) *models.ValidatorsActivityPageData {
	filterArgs := url.Values{}
	filterArgs.Add("group", fmt.Sprintf("%v", groupBy))

	pageData := &models.ValidatorsActivityPageData{
		ViewOptionGroupBy: groupBy,
		Sorting:           sortOrder,
	}
	logrus.Debugf("validators_activity page called: %v:%v [%v]", pageIdx, pageSize, groupBy)
	if pageIdx == 0 {
		pageData.IsDefaultPage = true
	}

	if pageSize > 100 {
		pageSize = 100
	}
	pageData.PageSize = pageSize
	pageData.CurrentPageIndex = pageIdx + 1
	if pageIdx >= 1 {
		pageData.PrevPageIndex = pageIdx
	}
	pageData.LastPageIndex = 0

	// group validators
	var cachedValidatorSet []beacon.ValidatorWithIndex
	if canonicalHead := services.GlobalBeaconService.GetBeaconIndexer().GetCanonicalHead(nil); canonicalHead != nil {
		cachedValidatorSet = services.GlobalBeaconService.GetBeaconIndexer().GetCachedValidatorSetForRoot(canonicalHead.Root)
	}
	cachedValidatorSetIdx := 0

	activeValidators := services.GlobalBeaconService.GetActiveValidatorData()
	activeValidatorIdx := 0
	validatorSetSize := services.GlobalBeaconService.GetBeaconIndexer().GetValidatorSetSize()
	currentEpoch := services.GlobalBeaconService.GetChainState().CurrentEpoch()
	validatorGroupMap := map[string]*models.ValidatorsActiviyPageDataGroup{}

	for vidx := uint64(0); vidx < validatorSetSize; vidx++ {
		var groupKey string
		var groupName string

		switch groupBy {
		case 1:
			groupIdx := vidx / 100000
			groupKey = fmt.Sprintf("%06d", groupIdx)
			groupName = fmt.Sprintf("%v - %v", groupIdx*100000, (groupIdx+1)*100000)
		case 2:
			groupIdx := vidx / 10000
			groupKey = fmt.Sprintf("%06d", groupIdx)
			groupName = fmt.Sprintf("%v - %v", groupIdx*10000, (groupIdx+1)*10000)
		case 3:
			groupName = services.GlobalBeaconService.GetValidatorName(vidx)
			groupKey = strings.ToLower(groupName)
		}

		validatorGroup := validatorGroupMap[groupKey]
		if validatorGroup == nil {
			validatorGroup = &models.ValidatorsActiviyPageDataGroup{
				Group:      groupName,
				GroupLower: groupKey,
				Validators: 0,
				Activated:  0,
				Online:     0,
				Offline:    0,
				Exited:     0,
				Slashed:    0,
			}
			validatorGroupMap[groupKey] = validatorGroup
		}

		var activeValidatorData *beacon.ValidatorData
		var validatorFlags uint16

		if activeValidatorIdx < len(activeValidators) && activeValidators[activeValidatorIdx].Index == vidx {
			activeValidatorData = activeValidators[activeValidatorIdx].Data
			activeValidatorIdx++
		}

		if cachedValidatorSetIdx < len(cachedValidatorSet) && cachedValidatorSet[cachedValidatorSetIdx].Index == vidx {
			cachedData := cachedValidatorSet[cachedValidatorSetIdx]
			activeValidatorData = &beacon.ValidatorData{
				ActivationEpoch:     cachedData.Validator.ActivationEpoch,
				ExitEpoch:           cachedData.Validator.ExitEpoch,
				EffectiveBalanceEth: uint16(cachedData.Validator.EffectiveBalance / beacon.EtherGweiFactor),
			}

			validatorFlags = beacon.GetValidatorStatusFlags(cachedData.Validator)

			cachedValidatorSetIdx++
		} else {
			validatorFlags = services.GlobalBeaconService.GetBeaconIndexer().GetValidatorFlags(phase0.ValidatorIndex(vidx))
		}

		validatorGroup.Validators++

		if validatorFlags&beacon.ValidatorStatusSlashed != 0 {
			validatorGroup.Slashed++
		}

		isExited := false
		if activeValidatorData != nil && activeValidatorData.ActivationEpoch <= currentEpoch {
			if activeValidatorData.ExitEpoch > currentEpoch {
				votingActivity := services.GlobalBeaconService.GetValidatorLiveness(phase0.ValidatorIndex(vidx), 3)

				validatorGroup.Activated++
				if votingActivity > 0 {
					validatorGroup.Online++
				} else {
					validatorGroup.Offline++
				}
			} else {
				isExited = true
			}
		} else if validatorFlags&beacon.ValidatorStatusExited != 0 {
			isExited = true
		}

		if isExited {
			validatorGroup.Exited++
		}
	}

	// sort / filter groups
	validatorGroups := maps.Values(validatorGroupMap)
	switch sortOrder {
	case "group":
		sort.Slice(validatorGroups, func(a, b int) bool {
			return strings.Compare(validatorGroups[a].GroupLower, validatorGroups[b].GroupLower) < 0
		})
		pageData.IsDefaultSorting = true
	case "group-d":
		sort.Slice(validatorGroups, func(a, b int) bool {
			return strings.Compare(validatorGroups[a].GroupLower, validatorGroups[b].GroupLower) > 0
		})
	case "count":
		sort.Slice(validatorGroups, func(a, b int) bool {
			return validatorGroups[a].Validators < validatorGroups[b].Validators
		})
	case "count-d":
		sort.Slice(validatorGroups, func(a, b int) bool {
			return validatorGroups[a].Validators > validatorGroups[b].Validators
		})
	case "active":
		sort.Slice(validatorGroups, func(a, b int) bool {
			return validatorGroups[a].Activated < validatorGroups[b].Activated
		})
	case "active-d":
		sort.Slice(validatorGroups, func(a, b int) bool {
			return validatorGroups[a].Activated > validatorGroups[b].Activated
		})
	case "online":
		sort.Slice(validatorGroups, func(a, b int) bool {
			return validatorGroups[a].Online < validatorGroups[b].Online
		})
	case "online-d":
		sort.Slice(validatorGroups, func(a, b int) bool {
			return validatorGroups[a].Online > validatorGroups[b].Online
		})
	case "offline":
		sort.Slice(validatorGroups, func(a, b int) bool {
			return validatorGroups[a].Offline < validatorGroups[b].Offline
		})
	case "offline-d":
		sort.Slice(validatorGroups, func(a, b int) bool {
			return validatorGroups[a].Offline > validatorGroups[b].Offline
		})
	case "exited":
		sort.Slice(validatorGroups, func(a, b int) bool {
			return validatorGroups[a].Exited < validatorGroups[b].Exited
		})
	case "exited-d":
		sort.Slice(validatorGroups, func(a, b int) bool {
			return validatorGroups[a].Exited > validatorGroups[b].Exited
		})
	case "slashed":
		sort.Slice(validatorGroups, func(a, b int) bool {
			return validatorGroups[a].Slashed < validatorGroups[b].Slashed
		})
	case "slashed-d":
		sort.Slice(validatorGroups, func(a, b int) bool {
			return validatorGroups[a].Slashed > validatorGroups[b].Slashed
		})
	}

	groupCount := uint64(len(validatorGroups))

	startIdx := pageIdx * pageSize
	endIdx := startIdx + pageSize
	if startIdx >= groupCount {
		validatorGroups = []*models.ValidatorsActiviyPageDataGroup{}
	} else if endIdx > groupCount {
		validatorGroups = validatorGroups[startIdx:]
	} else {
		validatorGroups = validatorGroups[startIdx:endIdx]
	}
	pageData.Groups = validatorGroups
	pageData.GroupCount = uint64(len(validatorGroups))

	pageData.TotalPages = groupCount / pageSize
	if groupCount%pageSize != 0 {
		pageData.TotalPages++
	}
	pageData.LastPageIndex = pageData.TotalPages - 1
	pageData.FirstGroup = startIdx
	pageData.LastGroup = endIdx

	if endIdx <= groupCount {
		pageData.NextPageIndex = pageIdx + 1
	}

	sortingArg := ""
	if sortOrder != "group" {
		sortingArg = fmt.Sprintf("&o=%v", sortOrder)
	}

	pageData.ViewPageLink = fmt.Sprintf("/validators/activity?%v&c=%v", filterArgs.Encode(), pageData.PageSize)
	pageData.FirstPageLink = fmt.Sprintf("/validators/activity?%v%v&c=%v", filterArgs.Encode(), sortingArg, pageData.PageSize)
	pageData.PrevPageLink = fmt.Sprintf("/validators/activity?%v%v&c=%v&s=%v", filterArgs.Encode(), sortingArg, pageData.PageSize, pageData.PrevPageIndex)
	pageData.NextPageLink = fmt.Sprintf("/validators/activity?%v%v&c=%v&s=%v", filterArgs.Encode(), sortingArg, pageData.PageSize, pageData.NextPageIndex)
	pageData.LastPageLink = fmt.Sprintf("/validators/activity?%v%v&c=%v&s=%v", filterArgs.Encode(), sortingArg, pageData.PageSize, pageData.LastPageIndex)

	return pageData
}
