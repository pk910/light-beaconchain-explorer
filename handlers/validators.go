package handlers

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	v1 "github.com/attestantio/go-eth2-client/api/v1"

	"github.com/ethpandaops/dora/dbtypes"
	"github.com/ethpandaops/dora/services"
	"github.com/ethpandaops/dora/templates"
	"github.com/ethpandaops/dora/types/models"
	"github.com/sirupsen/logrus"
)

// Validators will return the main "validators" page using a go template
func Validators(w http.ResponseWriter, r *http.Request) {
	var validatorsTemplateFiles = append(layoutTemplateFiles,
		"validators/validators.html",
		"_svg/professor.html",
	)

	var pageTemplate = templates.GetTemplate(validatorsTemplateFiles...)
	data := InitPageData(w, r, "validators", "/validators", "Validators", validatorsTemplateFiles)

	urlArgs := r.URL.Query()
	var pageNumber uint64 = 1
	if urlArgs.Has("p") {
		pageNumber, _ = strconv.ParseUint(urlArgs.Get("p"), 10, 64)
	}
	var pageSize uint64 = 50
	if urlArgs.Has("c") {
		pageSize, _ = strconv.ParseUint(urlArgs.Get("c"), 10, 64)
	}
	if urlArgs.Has("json") && pageSize > 10000 {
		pageSize = 10000
	} else if !urlArgs.Has("json") && pageSize > 1000 {
		pageSize = 1000
	}

	var filterPubKey string
	var filterIndex string
	var filterName string
	var filterStatus string
	if urlArgs.Has("f") {
		if urlArgs.Has("f.pubkey") {
			filterPubKey = urlArgs.Get("f.pubkey")
		}
		if urlArgs.Has("f.index") {
			filterIndex = urlArgs.Get("f.index")
		}
		if urlArgs.Has("f.name") {
			filterName = urlArgs.Get("f.name")
		}
		if urlArgs.Has("f.status") {
			filterStatus = strings.Join(urlArgs["f.status"], ",")
		}
	}
	var sortOrder string
	if urlArgs.Has("o") {
		sortOrder = urlArgs.Get("o")
	}

	var pageError error
	pageError = services.GlobalCallRateLimiter.CheckCallLimit(r, 1)
	if pageError == nil {
		data.Data, pageError = getValidatorsPageData(pageNumber, pageSize, sortOrder, filterPubKey, filterIndex, filterName, filterStatus)
	}
	if pageError != nil {
		handlePageError(w, r, pageError)
		return
	}

	if urlArgs.Has("json") {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(data.Data)
		if err != nil {
			logrus.WithError(err).Error("error encoding index data")
			http.Error(w, "Internal server error", http.StatusServiceUnavailable)
		}
	}

	w.Header().Set("Content-Type", "text/html")
	if handleTemplateError(w, r, "validators.go", "Validators", "", pageTemplate.ExecuteTemplate(w, "layout", data)) != nil {
		return // an error has occurred and was processed
	}
}

func getValidatorsPageData(pageNumber uint64, pageSize uint64, sortOrder string, filterPubKey string, filterIndex string, filterName string, filterStatus string) (*models.ValidatorsPageData, error) {
	pageData := &models.ValidatorsPageData{}
	pageCacheKey := fmt.Sprintf("validators:%v:%v:%v:%v:%v:%v:%v", pageNumber, pageSize, sortOrder, filterPubKey, filterIndex, filterName, filterStatus)
	pageRes, pageErr := services.GlobalFrontendCache.ProcessCachedPage(pageCacheKey, true, pageData, func(pageCall *services.FrontendCacheProcessingPage) interface{} {
		pageData, cacheTimeout := buildValidatorsPageData(pageNumber, pageSize, sortOrder, filterPubKey, filterIndex, filterName, filterStatus)
		pageCall.CacheTimeout = cacheTimeout
		return pageData
	})
	if pageErr == nil && pageRes != nil {
		resData, resOk := pageRes.(*models.ValidatorsPageData)
		if !resOk {
			return nil, ErrInvalidPageModel
		}
		pageData = resData
	}
	return pageData, pageErr
}

func buildValidatorsPageData(pageNumber uint64, pageSize uint64, sortOrder string, filterPubKey string, filterIndex string, filterName string, filterStatus string) (*models.ValidatorsPageData, time.Duration) {
	logrus.Debugf("validators page called: %v:%v:%v:%v:%v:%v:%v", pageNumber, pageSize, sortOrder, filterPubKey, filterIndex, filterName, filterStatus)
	pageData := &models.ValidatorsPageData{}
	cacheTime := 10 * time.Minute

	chainState := services.GlobalBeaconService.GetChainState()

	validatorFilter := dbtypes.ValidatorFilter{
		Limit:  pageSize,
		Offset: (pageNumber - 1) * pageSize,
	}

	filterArgs := url.Values{}
	if filterPubKey != "" || filterIndex != "" || filterName != "" || filterStatus != "" {
		if filterPubKey != "" {
			pageData.FilterPubKey = filterPubKey
			filterArgs.Add("f.pubkey", filterPubKey)
			filterPubKeyVal, _ := hex.DecodeString(strings.Replace(filterPubKey, "0x", "", -1))
			validatorFilter.PubKey = filterPubKeyVal
		}
		if filterIndex != "" {
			pageData.FilterIndex = filterIndex
			filterArgs.Add("f.index", filterIndex)
			filterIndexVal, _ := strconv.ParseUint(filterIndex, 10, 64)
			validatorFilter.MinIndex = &filterIndexVal
			validatorFilter.MaxIndex = &filterIndexVal
		}
		if filterName != "" {
			pageData.FilterName = filterName
			filterArgs.Add("f.name", filterName)
			validatorFilter.ValidatorName = filterName
		}
		if filterStatus != "" {
			pageData.FilterStatus = filterStatus
			filterArgs.Add("f.status", filterStatus)
			filterStatusVal := strings.Split(filterStatus, ",")
			validatorFilter.Status = make([]v1.ValidatorState, 0)
			for _, status := range filterStatusVal {
				statusVal := v1.ValidatorState(0)
				err := statusVal.UnmarshalJSON([]byte(fmt.Sprintf("\"%v\"", status)))
				if err == nil {
					validatorFilter.Status = append(validatorFilter.Status, statusVal)
				}
			}
		}
	}

	// apply sort order
	switch sortOrder {
	case "index-d":
		validatorFilter.OrderBy = dbtypes.ValidatorOrderIndexDesc
	case "pubkey":
		validatorFilter.OrderBy = dbtypes.ValidatorOrderPubKeyAsc
	case "pubkey-d":
		validatorFilter.OrderBy = dbtypes.ValidatorOrderPubKeyDesc
	case "balance":
		validatorFilter.OrderBy = dbtypes.ValidatorOrderBalanceAsc
	case "balance-d":
		validatorFilter.OrderBy = dbtypes.ValidatorOrderBalanceDesc
	case "activation":
		validatorFilter.OrderBy = dbtypes.ValidatorOrderActivationEpochAsc
	case "activation-d":
		validatorFilter.OrderBy = dbtypes.ValidatorOrderActivationEpochDesc
	case "exit":
		validatorFilter.OrderBy = dbtypes.ValidatorOrderExitEpochAsc
	case "exit-d":
		validatorFilter.OrderBy = dbtypes.ValidatorOrderExitEpochDesc
	default:
		validatorFilter.OrderBy = dbtypes.ValidatorOrderIndexAsc
		pageData.IsDefaultSorting = true
		sortOrder = "index"
	}
	pageData.Sorting = sortOrder

	// get latest validator set
	var validatorSet []v1.Validator
	validatorSetRsp, validatorSetLen := services.GlobalBeaconService.GetFilteredValidatorSet(&validatorFilter, true)
	if len(validatorSetRsp) == 0 {
		cacheTime = 5 * time.Minute
		validatorSet = []v1.Validator{}
	} else {
		validatorSet = validatorSetRsp
	}

	// get status options
	statusMap := map[v1.ValidatorState]uint64{}
	for _, val := range validatorSet {
		statusMap[val.Status]++
	}
	pageData.FilterStatusOpts = make([]models.ValidatorsPageDataStatusOption, 0)
	for status, count := range statusMap {
		pageData.FilterStatusOpts = append(pageData.FilterStatusOpts, models.ValidatorsPageDataStatusOption{
			Status: status.String(),
			Count:  count,
		})
	}
	sort.Slice(pageData.FilterStatusOpts, func(a, b int) bool {
		return strings.Compare(pageData.FilterStatusOpts[a].Status, pageData.FilterStatusOpts[b].Status) < 0
	})

	totalPages := validatorSetLen / pageSize
	if (validatorSetLen % pageSize) > 0 {
		totalPages++
	}
	if pageNumber == 0 {
		pageData.IsDefaultPage = true
	} else if pageNumber >= totalPages {
		if totalPages == 0 {
			pageNumber = 0
		} else {
			pageNumber = totalPages
		}
	}

	pageData.PageSize = pageSize
	pageData.TotalPages = totalPages
	pageData.CurrentPageIndex = pageNumber
	if pageNumber > 1 {
		pageData.PrevPageIndex = pageNumber - 1
	}
	if pageNumber < totalPages {
		pageData.NextPageIndex = pageNumber + 1
	}
	if totalPages > 1 {
		pageData.LastPageIndex = totalPages
	}

	// get validators
	pageData.Validators = make([]*models.ValidatorsPageDataValidator, 0)

	for _, validator := range validatorSet {
		if validator.Validator == nil {
			continue
		}

		validatorData := &models.ValidatorsPageDataValidator{
			Index:            uint64(validator.Index),
			Name:             services.GlobalBeaconService.GetValidatorName(uint64(validator.Index)),
			PublicKey:        validator.Validator.PublicKey[:],
			Balance:          uint64(validator.Balance),
			EffectiveBalance: uint64(validator.Validator.EffectiveBalance),
		}
		if strings.HasPrefix(validator.Status.String(), "pending") {
			validatorData.State = "Pending"
		} else if validator.Status == v1.ValidatorStateActiveOngoing {
			validatorData.State = "Active"
			validatorData.ShowUpcheck = true
		} else if validator.Status == v1.ValidatorStateActiveExiting {
			validatorData.State = "Exiting"
			validatorData.ShowUpcheck = true
		} else if validator.Status == v1.ValidatorStateActiveSlashed {
			validatorData.State = "Slashed"
			validatorData.ShowUpcheck = true
		} else if validator.Status == v1.ValidatorStateExitedUnslashed {
			validatorData.State = "Exited"
		} else if validator.Status == v1.ValidatorStateExitedSlashed {
			validatorData.State = "Slashed"
		} else {
			validatorData.State = validator.Status.String()
		}

		if validatorData.ShowUpcheck {
			validatorData.UpcheckActivity = uint8(services.GlobalBeaconService.GetValidatorLiveness(validator.Index, 3))
			validatorData.UpcheckMaximum = uint8(3)
		}

		if validator.Validator.ActivationEpoch < 18446744073709551615 {
			validatorData.ShowActivation = true
			validatorData.ActivationEpoch = uint64(validator.Validator.ActivationEpoch)
			validatorData.ActivationTs = chainState.EpochToTime(validator.Validator.ActivationEpoch)
		}
		if validator.Validator.ExitEpoch < 18446744073709551615 {
			validatorData.ShowExit = true
			validatorData.ExitEpoch = uint64(validator.Validator.ExitEpoch)
			validatorData.ExitTs = chainState.EpochToTime(validator.Validator.ExitEpoch)
		}
		if validator.Validator.WithdrawalCredentials[0] == 0x01 || validator.Validator.WithdrawalCredentials[0] == 0x02 {
			validatorData.ShowWithdrawAddress = true
			validatorData.WithdrawAddress = validator.Validator.WithdrawalCredentials[12:]
		}

		pageData.Validators = append(pageData.Validators, validatorData)
	}
	pageData.ValidatorCount = validatorSetLen
	pageData.FirstValidator = pageNumber * pageSize
	pageData.LastValidator = pageData.FirstValidator + uint64(len(pageData.Validators))
	pageData.FilteredPageLink = fmt.Sprintf("/validators?f&%v&c=%v", filterArgs.Encode(), pageData.PageSize)

	return pageData, cacheTime
}
