-- +goose Up
-- +goose StatementBegin

CREATE TABLE IF NOT EXISTS public."validators" (
    validator_index BIGINT NOT NULL,
    pubkey bytea NOT NULL,
    withdrawal_credentials bytea NOT NULL,
    effective_balance BIGINT NOT NULL,
    slashed BOOLEAN NOT NULL,
    activation_eligibility_epoch BIGINT NOT NULL,
    activation_epoch BIGINT NOT NULL,
    exit_epoch BIGINT NOT NULL,
    withdrawable_epoch BIGINT NOT NULL,
    CONSTRAINT validators_pkey PRIMARY KEY (validator_index)
);

CREATE INDEX IF NOT EXISTS "validators_pubkey_idx"
    ON public."validators" ("pubkey");

-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
SELECT 'NOT SUPPORTED';
-- +goose StatementEnd
