-- ============================================================================
-- Workshop-3: Streaming ETL with Apache Kafka and Machine Learning
-- Step 7 & Step 8: Database Design (PostgreSQL)
-- Tables: raw_happiness_events (Staging/Traceability), dim_country, dim_date,
--         fact_predictions (Star Schema Fact Table) + Analytical Views for Power BI
-- ============================================================================

-- 1. Raw / Staging Table for Traceability & Auditing
-- Preserves the original incoming Kafka event exactly as received before transformation/prediction.
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id SERIAL PRIMARY KEY,
    event_id VARCHAR(64),
    kafka_topic VARCHAR(128) NOT NULL DEFAULT 'happiness-events',
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    event_key VARCHAR(128),
    raw_payload TEXT NOT NULL,
    event_time TIMESTAMP,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processing_status VARCHAR(32) NOT NULL CHECK (
        processing_status IN (
            'RECEIVED',
            'VALID',
            'INVALID_SCHEMA',
            'INVALID_VALUES',
            'PREDICTION_ERROR'
        )
    ),
    validation_errors TEXT,
    CONSTRAINT uq_raw_kafka_coord UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_raw_events_status ON raw_happiness_events(processing_status);
CREATE INDEX IF NOT EXISTS idx_raw_events_event_id ON raw_happiness_events(event_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_uq_raw_kafka_coord
    ON raw_happiness_events(kafka_topic, kafka_partition, kafka_offset);

-- 2. Dimension Table: Country
CREATE TABLE IF NOT EXISTS dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(128) NOT NULL UNIQUE
);

-- 3. Dimension Table: Date (Historical Observation Year)
CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    observation_year INTEGER NOT NULL UNIQUE,
    decade VARCHAR(16) NOT NULL
);

-- 4. Fact Table: Real-Time ML Predictions
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id SERIAL PRIMARY KEY,
    raw_event_id INTEGER NOT NULL UNIQUE REFERENCES raw_happiness_events(raw_event_id) ON DELETE CASCADE,
    event_id VARCHAR(64) NOT NULL UNIQUE,
    country_id INTEGER NOT NULL REFERENCES dim_country(country_id),
    date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    gdp DOUBLE PRECISION NOT NULL,
    family DOUBLE PRECISION NOT NULL,
    health DOUBLE PRECISION NOT NULL,
    freedom DOUBLE PRECISION NOT NULL,
    generosity DOUBLE PRECISION NOT NULL,
    corruption DOUBLE PRECISION NOT NULL,
    actual_score DOUBLE PRECISION NOT NULL,
    predicted_score DOUBLE PRECISION NOT NULL,
    prediction_error DOUBLE PRECISION NOT NULL,
    is_test_set BOOLEAN NOT NULL DEFAULT FALSE,
    event_time TIMESTAMP NOT NULL,
    prediction_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Non-destructive migration for existing databases
ALTER TABLE fact_predictions ADD COLUMN IF NOT EXISTS is_test_set BOOLEAN NOT NULL DEFAULT FALSE;
CREATE UNIQUE INDEX IF NOT EXISTS idx_uq_fact_predictions_event_id ON fact_predictions(event_id);
CREATE INDEX IF NOT EXISTS idx_fact_predictions_country ON fact_predictions(country_id);
CREATE INDEX IF NOT EXISTS idx_fact_predictions_date ON fact_predictions(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_predictions_event_time ON fact_predictions(event_time);
CREATE INDEX IF NOT EXISTS idx_fact_predictions_is_test ON fact_predictions(is_test_set);

-- ============================================================================
-- Analytical Views for Power BI Dashboard (Step 9 KPIs)
-- ============================================================================

-- View 1: Global KPI Summary (KPI 1: Average Prediction Error — Overall & by Split)
DROP VIEW IF EXISTS vw_kpi_summary CASCADE;
CREATE OR REPLACE VIEW vw_kpi_summary AS
SELECT
    COUNT(fp.prediction_id) AS total_predictions,
    ROUND(AVG(fp.actual_score)::NUMERIC, 4) AS avg_actual_score,
    ROUND(AVG(fp.predicted_score)::NUMERIC, 4) AS avg_predicted_score,
    ROUND(AVG(fp.prediction_error)::NUMERIC, 4) AS mae_all_events,
    ROUND(AVG(fp.prediction_error) FILTER (WHERE fp.is_test_set = TRUE)::NUMERIC, 4) AS mae_test_set_only,
    ROUND(AVG(fp.prediction_error) FILTER (WHERE fp.is_test_set = FALSE)::NUMERIC, 4) AS mae_train_set_only,
    ROUND(SQRT(AVG(POWER(fp.actual_score - fp.predicted_score, 2)))::NUMERIC, 4) AS rmse_all_events,
    ROUND(SQRT(AVG(POWER(fp.actual_score - fp.predicted_score, 2)) FILTER (WHERE fp.is_test_set = TRUE))::NUMERIC, 4) AS rmse_test_set_only,
    ROUND(MIN(fp.prediction_error)::NUMERIC, 4) AS min_prediction_error,
    ROUND(MAX(fp.prediction_error)::NUMERIC, 4) AS max_prediction_error
FROM fact_predictions fp;

-- View 2: Predictions by Country (KPI 2)
DROP VIEW IF EXISTS vw_predictions_by_country CASCADE;
CREATE OR REPLACE VIEW vw_predictions_by_country AS
SELECT
    dc.country_id,
    dc.country_name AS country,
    COUNT(fp.prediction_id) AS total_records,
    ROUND(AVG(fp.actual_score)::NUMERIC, 4) AS avg_actual_score,
    ROUND(AVG(fp.predicted_score)::NUMERIC, 4) AS avg_predicted_score,
    ROUND(AVG(fp.prediction_error)::NUMERIC, 4) AS avg_prediction_error,
    ROUND(AVG(fp.gdp)::NUMERIC, 4) AS avg_gdp,
    ROUND(AVG(fp.family)::NUMERIC, 4) AS avg_family,
    ROUND(AVG(fp.health)::NUMERIC, 4) AS avg_health
FROM fact_predictions fp
JOIN dim_country dc ON fp.country_id = dc.country_id
GROUP BY dc.country_id, dc.country_name;

-- View 3: Predicted vs Actual Score Detail (KPI 3)
DROP VIEW IF EXISTS vw_predicted_vs_actual CASCADE;
CREATE OR REPLACE VIEW vw_predicted_vs_actual AS
SELECT
    fp.prediction_id,
    fp.raw_event_id,
    fp.event_id,
    dc.country_name AS country,
    dd.observation_year AS year,
    dd.decade,
    fp.is_test_set,
    CASE WHEN fp.is_test_set THEN 'Test (30%)' ELSE 'Train (70%)' END AS dataset_split,
    fp.gdp,
    fp.family,
    fp.health,
    fp.freedom,
    fp.generosity,
    fp.corruption,
    fp.actual_score,
    fp.predicted_score,
    ROUND((fp.actual_score - fp.predicted_score)::NUMERIC, 4) AS residual_signed,
    fp.prediction_error,
    fp.event_time,
    fp.prediction_timestamp
FROM fact_predictions fp
JOIN dim_country dc ON fp.country_id = dc.country_id
JOIN dim_date dd ON fp.date_id = dd.date_id;

-- View 4: Prediction Trends Over Time (KPI 4)
DROP VIEW IF EXISTS vw_prediction_trends_over_time CASCADE;
CREATE OR REPLACE VIEW vw_prediction_trends_over_time AS
SELECT
    dd.observation_year AS year,
    dd.decade,
    COUNT(fp.prediction_id) AS countries_evaluated,
    ROUND(AVG(fp.actual_score)::NUMERIC, 4) AS avg_actual_score,
    ROUND(AVG(fp.predicted_score)::NUMERIC, 4) AS avg_predicted_score,
    ROUND(AVG(fp.prediction_error)::NUMERIC, 4) AS avg_prediction_error
FROM fact_predictions fp
JOIN dim_date dd ON fp.date_id = dd.date_id
GROUP BY dd.observation_year, dd.decade
ORDER BY dd.observation_year ASC;

-- View 5: Raw Event Contract Audit Summary
DROP VIEW IF EXISTS vw_event_audit_status CASCADE;
CREATE OR REPLACE VIEW vw_event_audit_status AS
SELECT
    processing_status,
    COUNT(*) AS total_events,
    ROUND((COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER (), 0))::NUMERIC, 2) AS percentage
FROM raw_happiness_events
GROUP BY processing_status;
