package db

import (
	"fmt"
	"math"
	"strings"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/ethpandaops/dora/dbtypes"
	"github.com/jmoiron/sqlx"
)

// InsertValidator inserts a single validator into the database
func InsertValidator(validator *dbtypes.Validator, tx *sqlx.Tx) error {
	_, err := tx.Exec(EngineQuery(map[dbtypes.DBEngineType]string{
		dbtypes.DBEnginePgsql: `
			INSERT INTO validators (
				validator_index, pubkey, withdrawal_credentials, effective_balance,
				slashed, activation_eligibility_epoch, activation_epoch,
				exit_epoch, withdrawable_epoch
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (validator_index) DO UPDATE SET
				withdrawal_credentials = excluded.withdrawal_credentials,
				effective_balance = excluded.effective_balance,
				slashed = excluded.slashed,
				activation_eligibility_epoch = excluded.activation_eligibility_epoch,
				activation_epoch = excluded.activation_epoch,
				exit_epoch = excluded.exit_epoch,
				withdrawable_epoch = excluded.withdrawable_epoch`,
		dbtypes.DBEngineSqlite: `
			INSERT OR REPLACE INTO validators (
				validator_index, pubkey, withdrawal_credentials, effective_balance,
				slashed, activation_eligibility_epoch, activation_epoch,
				exit_epoch, withdrawable_epoch
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
	}),
		validator.ValidatorIndex,
		validator.Pubkey,
		validator.WithdrawalCredentials,
		validator.EffectiveBalance,
		validator.Slashed,
		validator.ActivationEligibilityEpoch,
		validator.ActivationEpoch,
		validator.ExitEpoch,
		validator.WithdrawableEpoch)

	if err != nil {
		return fmt.Errorf("error inserting validator: %v", err)
	}
	return nil
}

// InsertValidatorBatch inserts multiple validators in a batch
func InsertValidatorBatch(validators []*dbtypes.Validator, tx *sqlx.Tx) error {
	if len(validators) == 0 {
		return nil
	}

	valueStrings := make([]string, len(validators))
	valueArgs := make([]interface{}, 0, len(validators)*9)
	for i, val := range validators {
		valueStrings[i] = fmt.Sprintf("($%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v)",
			i*9+1, i*9+2, i*9+3, i*9+4, i*9+5, i*9+6, i*9+7, i*9+8, i*9+9)
		valueArgs = append(valueArgs,
			val.ValidatorIndex,
			val.Pubkey,
			val.WithdrawalCredentials,
			val.EffectiveBalance,
			val.Slashed,
			val.ActivationEligibilityEpoch,
			val.ActivationEpoch,
			val.ExitEpoch,
			val.WithdrawableEpoch)
	}

	stmt := fmt.Sprintf(EngineQuery(map[dbtypes.DBEngineType]string{
		dbtypes.DBEnginePgsql: `
			INSERT INTO validators (
				validator_index, pubkey, withdrawal_credentials, effective_balance,
				slashed, activation_eligibility_epoch, activation_epoch,
				exit_epoch, withdrawable_epoch
			) VALUES %s
			ON CONFLICT (validator_index) DO UPDATE SET
				withdrawal_credentials = excluded.withdrawal_credentials,
				effective_balance = excluded.effective_balance,
				slashed = excluded.slashed,
				activation_eligibility_epoch = excluded.activation_eligibility_epoch,
				activation_epoch = excluded.activation_epoch,
				exit_epoch = excluded.exit_epoch,
				withdrawable_epoch = excluded.withdrawable_epoch`,
		dbtypes.DBEngineSqlite: `
			INSERT OR REPLACE INTO validators (
				validator_index, pubkey, withdrawal_credentials, effective_balance,
				slashed, activation_eligibility_epoch, activation_epoch,
				exit_epoch, withdrawable_epoch
			) VALUES %s`,
	}), strings.Join(valueStrings, ","))

	_, err := tx.Exec(stmt, valueArgs...)
	if err != nil {
		return fmt.Errorf("error inserting validator batch: %v", err)
	}

	return nil
}

// GetValidatorByIndex returns a validator by index
func GetValidatorByIndex(index phase0.ValidatorIndex) *dbtypes.Validator {
	validator := dbtypes.Validator{}
	err := ReaderDb.Get(&validator, `
		SELECT * FROM validators WHERE validator_index = $1
	`, index)
	if err != nil {
		return nil
	}
	return &validator
}

// GetValidatorByPubkey returns a validator by pubkey
func GetValidatorByPubkey(pubkey []byte) *dbtypes.Validator {
	validator := dbtypes.Validator{}
	err := ReaderDb.Get(&validator, `
		SELECT * FROM validators WHERE pubkey = $1
	`, pubkey)
	if err != nil {
		return nil
	}
	return &validator
}

// GetValidatorRange returns validators in a given index range
func GetValidatorRange(startIndex uint64, endIndex uint64) []*dbtypes.Validator {
	validators := []*dbtypes.Validator{}
	err := ReaderDb.Select(&validators, `
		SELECT * FROM validators 
		WHERE validator_index >= $1 AND validator_index <= $2
		ORDER BY validator_index ASC
	`, startIndex, endIndex)
	if err != nil {
		logger.Errorf("Error while fetching validator range: %v", err)
		return nil
	}
	return validators
}

// GetMaxValidatorIndex returns the highest validator index in the database
func GetMaxValidatorIndex() (uint64, error) {
	var maxIndex uint64
	err := ReaderDb.Get(&maxIndex, "SELECT COALESCE(MAX(validator_index), 0) FROM validators")
	if err != nil {
		return 0, fmt.Errorf("error getting max validator index: %v", err)
	}
	return maxIndex, nil
}

// GetValidatorIndexesByFilter returns validator indexes matching a filter
func GetValidatorIndexesByFilter(filter dbtypes.ValidatorFilter, currentEpoch uint64) ([]uint64, error) {
	var sql strings.Builder
	args := []interface{}{}
	fmt.Fprint(&sql, `
	SELECT
		validator_index
	FROM validators
	`)

	args = buildValidatorFilterSql(filter, currentEpoch, &sql, args)

	switch filter.OrderBy {
	case dbtypes.ValidatorOrderIndexAsc:
		fmt.Fprintf(&sql, " ORDER BY validator_index ASC")
	case dbtypes.ValidatorOrderIndexDesc:
		fmt.Fprintf(&sql, " ORDER BY validator_index DESC")
	case dbtypes.ValidatorOrderPubKeyAsc:
		fmt.Fprintf(&sql, " ORDER BY pubkey ASC")
	case dbtypes.ValidatorOrderPubKeyDesc:
		fmt.Fprintf(&sql, " ORDER BY pubkey DESC")
	case dbtypes.ValidatorOrderBalanceAsc:
		fmt.Fprintf(&sql, " ORDER BY effective_balance ASC")
	case dbtypes.ValidatorOrderBalanceDesc:
		fmt.Fprintf(&sql, " ORDER BY effective_balance DESC")
	case dbtypes.ValidatorOrderActivationEpochAsc:
		fmt.Fprintf(&sql, " ORDER BY activation_epoch ASC")
	case dbtypes.ValidatorOrderActivationEpochDesc:
		fmt.Fprintf(&sql, " ORDER BY activation_epoch DESC")
	case dbtypes.ValidatorOrderExitEpochAsc:
		fmt.Fprintf(&sql, " ORDER BY exit_epoch ASC")
	case dbtypes.ValidatorOrderExitEpochDesc:
		fmt.Fprintf(&sql, " ORDER BY exit_epoch DESC")
	case dbtypes.ValidatorOrderWithdrawableEpochAsc:
		fmt.Fprintf(&sql, " ORDER BY withdrawable_epoch ASC")
	case dbtypes.ValidatorOrderWithdrawableEpochDesc:
		fmt.Fprintf(&sql, " ORDER BY withdrawable_epoch DESC")
	}

	validatorIds := []uint64{}
	err := ReaderDb.Select(&validatorIds, sql.String(), args...)
	if err != nil {
		logger.Errorf("Error while fetching validators by filter: %v", err)
		return nil, err
	}

	return validatorIds, nil
}

func buildValidatorFilterSql(filter dbtypes.ValidatorFilter, currentEpoch uint64, sql *strings.Builder, args []interface{}) []interface{} {
	if filter.ValidatorName != "" {
		fmt.Fprintf(sql, ` LEFT JOIN validator_names ON validator_names."index" = validators.validator_index `)
	}

	filterOp := "WHERE"
	if filter.MinIndex != nil {
		args = append(args, *filter.MinIndex)
		fmt.Fprintf(sql, " %v validator_index >= $%v", filterOp, len(args))
		filterOp = "AND"
	}
	if filter.MaxIndex != nil {
		args = append(args, *filter.MaxIndex)
		fmt.Fprintf(sql, " %v validator_index <= $%v", filterOp, len(args))
		filterOp = "AND"
	}
	if filter.PubKey != nil {
		args = append(args, filter.PubKey)
		fmt.Fprintf(sql, " %v pubkey = $%v", filterOp, len(args))
		filterOp = "AND"
	}
	if filter.WithdrawalAddress != nil {
		wdcreds1 := make([]byte, 32)
		wdcreds1[0] = 0x01
		copy(wdcreds1[12:], filter.WithdrawalAddress)
		wdcreds2 := make([]byte, 32)
		wdcreds2[0] = 0x02
		copy(wdcreds2[12:], filter.WithdrawalAddress)
		args = append(args, wdcreds1, wdcreds2)
		fmt.Fprintf(sql, " %v (withdrawal_credentials = $%v OR withdrawal_credentials = $%v)", filterOp, len(args)-1, len(args))
		filterOp = "AND"
	}
	if filter.ValidatorName != "" {
		args = append(args, "%"+filter.ValidatorName+"%")
		fmt.Fprintf(sql, EngineQuery(map[dbtypes.DBEngineType]string{
			dbtypes.DBEnginePgsql:  ` %v validator_names.name ilike $%v `,
			dbtypes.DBEngineSqlite: ` %v validator_names.name LIKE $%v `,
		}), filterOp, len(args))
		filterOp = "AND"
	}
	if len(filter.Status) > 0 {
		values := []string{}
		for _, status := range filter.Status {
			args = append(args, status)
			values = append(values, fmt.Sprintf("$%v", len(args)))
		}
		fmt.Fprintf(sql, " %v %v IN (%s)", filterOp, buildValidatorStatusSql(currentEpoch), strings.Join(values, ","))
		filterOp = "AND"
	}

	return args
}

func buildValidatorStatusSql(currentEpoch uint64) string {
	return fmt.Sprintf(`
		SWITCH (
			WHEN activation_eligibility_epoch == %v THEN 1
			WHEN activation_epoch > %v THEN 2
			WHEN exit_epoch == %v THEN 3
			WHEN exit_epoch > %v AND slashed THEN 5
			WHEN exit_epoch > %v THEN 4
			WHEN withdrawable_epoch > %v AND slashed THEN 7
			WHEN withdrawable_epoch > %v THEN 6
			WHEN effective_balance == 0 THEN 9
			ELSE 8
		)
	`, math.MaxInt64, currentEpoch, math.MaxInt64, currentEpoch, currentEpoch, currentEpoch, currentEpoch)
}

func StreamValidatorsByIndexes(indexes []uint64, cb func(validator *dbtypes.Validator) bool) error {
	const batchSize = 1000

	// Process in batches
	for i := 0; i < len(indexes); i += batchSize {
		end := i + batchSize
		if end > len(indexes) {
			end = len(indexes)
		}
		batch := indexes[i:end]

		var sql strings.Builder
		fmt.Fprintf(&sql, `
		SELECT
			validator_index, pubkey, withdrawal_credentials, effective_balance,
			slashed, activation_eligibility_epoch, activation_epoch,
			exit_epoch, withdrawable_epoch
		FROM validators
		WHERE validator_index in (`)

		args := make([]any, len(batch))
		for j, index := range batch {
			if j > 0 {
				fmt.Fprintf(&sql, ", ")
			}
			fmt.Fprintf(&sql, "$%v", j+1)
			args[j] = index
		}
		fmt.Fprintf(&sql, ")")

		// Create index map for ordering
		indexMap := make(map[uint64]int, len(batch))
		for pos, idx := range batch {
			indexMap[idx] = pos
		}

		// Fetch all validators for this batch
		validators := make([]*dbtypes.Validator, len(batch))
		rows, err := ReaderDb.Query(sql.String(), args...)
		if err != nil {
			return fmt.Errorf("error querying validators: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			validator := &dbtypes.Validator{}
			err := rows.Scan(
				&validator.ValidatorIndex,
				&validator.Pubkey,
				&validator.WithdrawalCredentials,
				&validator.EffectiveBalance,
				&validator.Slashed,
				&validator.ActivationEligibilityEpoch,
				&validator.ActivationEpoch,
				&validator.ExitEpoch,
				&validator.WithdrawableEpoch,
			)
			if err != nil {
				return fmt.Errorf("error scanning validator: %v", err)
			}
			pos := indexMap[uint64(validator.ValidatorIndex)]
			validators[pos] = validator
		}

		if err = rows.Err(); err != nil {
			return fmt.Errorf("error iterating rows: %v", err)
		}

		// Stream in original order
		for _, v := range validators {
			if v != nil && !cb(v) {
				return nil
			}
		}
	}

	return nil
}
