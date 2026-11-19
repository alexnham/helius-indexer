-- A first observation's payload is the payload `captures` holds for its signature, so it
-- is no longer written twice: NULL payload columns mean "byte-identical to the current
-- captures row". Every write that changes a captures payload first copies it into these
-- NULL rows (`materialize_inputs`); a captures payload must never change any other way.
-- Existing rows keep their bytes. Rollback: fill NULL rows from captures, then restore
-- NOT NULL on both columns and drop the check.
ALTER TABLE capture_inputs ALTER COLUMN result_json DROP NOT NULL;
ALTER TABLE capture_inputs ALTER COLUMN capture_json DROP NOT NULL;
ALTER TABLE capture_inputs ADD CONSTRAINT capture_inputs_payload_check
    CHECK ((result_json IS NULL) = (capture_json IS NULL));
