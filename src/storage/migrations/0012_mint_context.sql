-- An exact dataset may fix its indexed mint once; transfers of other mints in the same
-- transactions are kept as context, never as indexed transfers.
ALTER TABLE dataset_policy DROP CONSTRAINT dataset_policy_check;
ALTER TABLE dataset_policy ADD CONSTRAINT dataset_policy_check CHECK (
    (policy = 'exact' AND percent = 100) OR (policy = 'sha256-transfer-id-v1' AND mint IS NOT NULL)
);

-- Other mints' transfers in a transaction with at least one stored indexed transfer.
-- No coverage guarantee: these mints are seen only where the indexed mint is.
CREATE TABLE transfer_context (
    transfer_id TEXT PRIMARY KEY,
    signature TEXT NOT NULL,
    slot BIGINT NOT NULL,
    mint TEXT NOT NULL,
    record_json TEXT NOT NULL
);
CREATE INDEX transfer_context_signature ON transfer_context(signature);
