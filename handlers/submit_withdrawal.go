package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	v1 "github.com/attestantio/go-eth2-client/api/v1"
	"github.com/ethereum/go-ethereum/common"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/dora/dbtypes"
	"github.com/ethpandaops/dora/indexer/execution"
	"github.com/ethpandaops/dora/services"
	"github.com/ethpandaops/dora/templates"
	"github.com/ethpandaops/dora/types/models"
	"github.com/ethpandaops/dora/utils"
)

// SubmitWithdrawal will submit a withdrawal request
func SubmitWithdrawal(w http.ResponseWriter, r *http.Request) {
	var submitWithdrawalTemplateFiles = append(layoutTemplateFiles,
		"submit_withdrawal/submit_withdrawal.html",
	)
	var pageTemplate = templates.GetTemplate(submitWithdrawalTemplateFiles...)

	if !utils.Config.Frontend.ShowSubmitElRequests {
		handlePageError(w, r, errors.New("submit el requests is not enabled"))
		return
	}

	query := r.URL.Query()
	if query.Has("ajax") {
		err := handleSubmitWithdrawalPageDataAjax(w, r)
		if err != nil {
			handlePageError(w, r, err)
		}
		return
	}

	pageData, pageError := getSubmitWithdrawalPageData()
	if pageError != nil {
		handlePageError(w, r, pageError)
		return
	}
	if pageData == nil {
		data := InitPageData(w, r, "blockchain", "/submit_withdrawal", "Submit Withdrawals & Exits", submitWithdrawalTemplateFiles)
		w.Header().Set("Content-Type", "text/html")
		if handleTemplateError(w, r, "submit_withdrawal.go", "Submit Withdrawals & Exits", "", pageTemplate.ExecuteTemplate(w, "layout", data)) != nil {
			return // an error has occurred and was processed
		}
		return
	}

	data := InitPageData(w, r, "blockchain", "/submit_withdrawal", "Submit Withdrawals & Exits", submitWithdrawalTemplateFiles)
	data.Data = pageData
	w.Header().Set("Content-Type", "text/html")
	if handleTemplateError(w, r, "submit_withdrawal.go", "Submit Withdrawals & Exits", "", pageTemplate.ExecuteTemplate(w, "layout", data)) != nil {
		return // an error has occurred and was processed
	}
}

func getSubmitWithdrawalPageData() (*models.SubmitWithdrawalPageData, error) {
	pageData := &models.SubmitWithdrawalPageData{}
	pageCacheKey := "submit_withdrawal"
	pageRes, pageErr := services.GlobalFrontendCache.ProcessCachedPage(pageCacheKey, true, pageData, func(pageCall *services.FrontendCacheProcessingPage) interface{} {
		pageData, cacheTimeout := buildSubmitWithdrawalPageData()
		pageCall.CacheTimeout = cacheTimeout
		return pageData
	})
	if pageErr == nil && pageRes != nil {
		resData, resOk := pageRes.(*models.SubmitWithdrawalPageData)
		if !resOk {
			return nil, ErrInvalidPageModel
		}
		pageData = resData
	}
	return pageData, pageErr
}

func buildSubmitWithdrawalPageData() (*models.SubmitWithdrawalPageData, time.Duration) {
	logrus.Debugf("submit withdrawal page called")

	chainState := services.GlobalBeaconService.GetChainState()
	specs := chainState.GetSpecs()

	pageData := &models.SubmitWithdrawalPageData{
		NetworkName:         specs.ConfigName,
		PublicRPCUrl:        utils.Config.Frontend.PublicRPCUrl,
		RainbowkitProjectId: utils.Config.Frontend.RainbowkitProjectId,
		ChainId:             specs.DepositChainId,
		WithdrawalContract:  execution.WithdrawalContractAddr,
		ExplorerUrl:         utils.Config.Frontend.EthExplorerLink,
		MinValidatorBalance: specs.MinActivationBalance,
	}

	return pageData, 1 * time.Hour
}

func handleSubmitWithdrawalPageDataAjax(w http.ResponseWriter, r *http.Request) error {
	query := r.URL.Query()
	var pageData interface{}

	switch query.Get("ajax") {
	case "load_validators":
		address := query.Get("address")
		addressBytes := common.HexToAddress(address)

		validators, _ := services.GlobalBeaconService.GetFilteredValidatorSet(&dbtypes.ValidatorFilter{
			WithdrawalAddress: addressBytes[:],
		}, true)

		result := []models.SubmitWithdrawalPageDataValidator{}
		for _, validator := range validators {
			var status string
			if strings.HasPrefix(validator.Status.String(), "pending") {
				status = "Pending"
			} else if validator.Status == v1.ValidatorStateActiveOngoing {
				status = "Active"
			} else if validator.Status == v1.ValidatorStateActiveExiting {
				status = "Exiting"
			} else if validator.Status == v1.ValidatorStateActiveSlashed {
				status = "Slashed"
			} else if validator.Status == v1.ValidatorStateExitedUnslashed {
				status = "Exited"
			} else if validator.Status == v1.ValidatorStateExitedSlashed {
				status = "Slashed"
			} else {
				status = validator.Status.String()
			}

			result = append(result, models.SubmitWithdrawalPageDataValidator{
				Index:    uint64(validator.Index),
				Pubkey:   validator.Validator.PublicKey.String(),
				Balance:  uint64(validator.Balance),
				CredType: fmt.Sprintf("%02x", validator.Validator.WithdrawalCredentials[0]),
				Status:   status,
			})
		}

		pageData = result
	default:
		return errors.New("invalid ajax request")
	}

	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(pageData)
	if err != nil {
		logrus.WithError(err).Error("error encoding index data")
		http.Error(w, "Internal server error", http.StatusServiceUnavailable)
	}
	return nil
}
