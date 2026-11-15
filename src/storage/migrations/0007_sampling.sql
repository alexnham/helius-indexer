-- Existing datasets remain exact. A fresh session may opt into the demo policy once.
CREATE TABLE dataset_policy (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    policy TEXT NOT NULL CHECK (policy IN ('exact', 'sha256-transfer-id-v1')),
    mint TEXT,
    CHECK ((policy = 'exact' AND mint IS NULL) OR (policy = 'sha256-transfer-id-v1' AND mint IS NOT NULL))
);
INSERT INTO dataset_policy(singleton, policy, mint) VALUES(TRUE, 'exact', NULL);

-- Serialize activation with the first admission of durable inputs and progress.
CREATE FUNCTION lock_dataset_policy() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM dataset_policy WHERE singleton FOR SHARE;
    RETURN NULL;
END $$;
CREATE TRIGGER captures_policy BEFORE INSERT ON captures FOR EACH STATEMENT EXECUTE FUNCTION lock_dataset_policy();
CREATE TRIGGER live_raw_policy BEFORE INSERT ON live_raw FOR EACH STATEMENT EXECUTE FUNCTION lock_dataset_policy();
CREATE TRIGGER live_runs_policy BEFORE INSERT ON live_runs FOR EACH STATEMENT EXECUTE FUNCTION lock_dataset_policy();
CREATE TRIGGER historical_slots_policy BEFORE INSERT ON historical_slots FOR EACH STATEMENT EXECUTE FUNCTION lock_dataset_policy();
CREATE TRIGGER historical_inputs_policy BEFORE INSERT ON historical_inputs FOR EACH STATEMENT EXECUTE FUNCTION lock_dataset_policy();
