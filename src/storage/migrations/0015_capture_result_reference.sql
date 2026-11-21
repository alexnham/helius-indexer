-- A NULL captures.result_json is the result inside capture_json, so a capture's
-- transaction is stored once; a reference that differs (enriched by a later compatible
-- observation or a repair) is stored explicitly. Readers use the same expression
-- (`storage::RESULT_JSON`). Existing rows keep their explicit references.
-- Rollback: UPDATE captures SET result_json=(convert_from(capture_json,'UTF8')::jsonb->'result')::text
--   WHERE result_json IS NULL; then SET NOT NULL and decrement the schema version.
ALTER TABLE captures ALTER COLUMN result_json DROP NOT NULL;
