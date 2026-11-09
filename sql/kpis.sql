-- ============================================================================
-- Workshop-3: Streaming ETL with Apache Kafka and Machine Learning
-- Step 9: Analytical Queries & Minimum Required KPIs (sql/kpis.sql)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- KPI 1A: Average Prediction Error (Global & Split by Train 70% vs Test 30%)
-- Shows overall MAE/RMSE alongside strict out-of-sample Test (30%) MAE/RMSE.
-- ----------------------------------------------------------------------------
SELECT
    CASE
        WHEN GROUPING(is_test_set) = 1 THEN 'ALL EVENTS (100%)'
        WHEN is_test_set = TRUE THEN 'TEST SET ONLY (30% Out-of-Sample)'
        ELSE 'TRAIN SET ONLY (70% In-Sample)'
    END AS evaluation_subset,
    COUNT(prediction_id) AS total_predictions,
    ROUND(AVG(actual_score)::NUMERIC, 4) AS avg_actual_score,
    ROUND(AVG(predicted_score)::NUMERIC, 4) AS avg_predicted_score,
    ROUND(AVG(prediction_error)::NUMERIC, 4) AS average_prediction_error_mae,
    ROUND(SQRT(AVG(POWER(actual_score - predicted_score, 2)))::NUMERIC, 4) AS rmse_error,
    ROUND(MIN(prediction_error)::NUMERIC, 4) AS min_error,
    ROUND(MAX(prediction_error)::NUMERIC, 4) AS max_error
FROM fact_predictions
GROUP BY ROLLUP(is_test_set)
ORDER BY GROUPING(is_test_set) DESC, is_test_set DESC;


-- ----------------------------------------------------------------------------
-- KPI 2: Predictions by Country
-- Aggregates predicted score, actual score, and average prediction error per country.
-- ----------------------------------------------------------------------------
SELECT
    dc.country_name AS country,
    COUNT(fp.prediction_id) AS years_observed,
    ROUND(AVG(fp.actual_score)::NUMERIC, 4) AS avg_actual_score,
    ROUND(AVG(fp.predicted_score)::NUMERIC, 4) AS avg_predicted_score,
    ROUND(AVG(fp.prediction_error)::NUMERIC, 4) AS avg_prediction_error,
    ROUND(MIN(fp.predicted_score)::NUMERIC, 4) AS min_predicted_score,
    ROUND(MAX(fp.predicted_score)::NUMERIC, 4) AS max_predicted_score
FROM fact_predictions fp
JOIN dim_country dc ON fp.country_id = dc.country_id
GROUP BY dc.country_name
ORDER BY avg_predicted_score DESC;


-- ----------------------------------------------------------------------------
-- KPI 3: Predicted vs Actual Score (Event-Level Comparison & Residuals)
-- Compares each prediction against the ground-truth happiness score.
-- ----------------------------------------------------------------------------
SELECT
    fp.prediction_id,
    fp.raw_event_id,
    fp.event_id,
    dc.country_name AS country,
    dd.observation_year AS year,
    CASE WHEN fp.is_test_set THEN 'Test (30%)' ELSE 'Train (70%)' END AS dataset_split,
    ROUND(fp.actual_score::NUMERIC, 4) AS actual_score,
    ROUND(fp.predicted_score::NUMERIC, 4) AS predicted_score,
    ROUND((fp.actual_score - fp.predicted_score)::NUMERIC, 4) AS signed_residual,
    ROUND(fp.prediction_error::NUMERIC, 4) AS prediction_error,
    fp.event_time,
    fp.prediction_timestamp
FROM fact_predictions fp
JOIN dim_country dc ON fp.country_id = dc.country_id
JOIN dim_date dd ON fp.date_id = dd.date_id
ORDER BY fp.prediction_id ASC;


-- ----------------------------------------------------------------------------
-- KPI 4A: Prediction Trends Over Time (By Historical Observation Year 2015-2019)
-- Evaluates how actual vs predicted happiness scores and errors evolve across years.
-- ----------------------------------------------------------------------------
SELECT
    dd.observation_year AS year,
    dd.decade,
    COUNT(fp.prediction_id) AS total_countries,
    ROUND(AVG(fp.actual_score)::NUMERIC, 4) AS avg_actual_score,
    ROUND(AVG(fp.predicted_score)::NUMERIC, 4) AS avg_predicted_score,
    ROUND(AVG(fp.prediction_error)::NUMERIC, 4) AS avg_prediction_error
FROM fact_predictions fp
JOIN dim_date dd ON fp.date_id = dd.date_id
GROUP BY dd.observation_year, dd.decade
ORDER BY dd.observation_year ASC;


-- ----------------------------------------------------------------------------
-- KPI 4B: Streaming Prediction Trends Over Time (By Ingestion / Event Timestamp)
-- Tracks real-time throughput and average prediction error per streaming window.
-- ----------------------------------------------------------------------------
SELECT
    DATE_TRUNC('second', fp.prediction_timestamp) AS stream_window,
    COUNT(fp.prediction_id) AS events_scored,
    ROUND(AVG(fp.actual_score)::NUMERIC, 4) AS avg_actual_score,
    ROUND(AVG(fp.predicted_score)::NUMERIC, 4) AS avg_predicted_score,
    ROUND(AVG(fp.prediction_error)::NUMERIC, 4) AS avg_prediction_error
FROM fact_predictions fp
GROUP BY DATE_TRUNC('second', fp.prediction_timestamp)
ORDER BY stream_window ASC;


-- ----------------------------------------------------------------------------
-- KPI 5 (Audit & Traceability): Raw Event Contract Validation Breakdown
-- Verifies how many Kafka events passed validation vs failed (VALID, INVALID_SCHEMA,
-- INVALID_VALUES, PREDICTION_ERROR).
-- ----------------------------------------------------------------------------
SELECT
    processing_status,
    COUNT(*) AS event_count,
    ROUND((COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER (), 0))::NUMERIC, 2) AS pct_of_stream
FROM raw_happiness_events
GROUP BY processing_status
ORDER BY event_count DESC;


-- ----------------------------------------------------------------------------
-- KPI 6 (End-to-End Lineage): Trace Fact Predictions Back to Original Raw Payload
-- Demonstrates full traceability via raw_event_id foreign key.
-- ----------------------------------------------------------------------------
SELECT
    fp.prediction_id,
    fp.raw_event_id,
    r.event_id,
    r.kafka_partition,
    r.kafka_offset,
    r.processing_status,
    dc.country_name AS country,
    dd.observation_year AS year,
    fp.is_test_set,
    fp.actual_score,
    fp.predicted_score,
    fp.prediction_error,
    r.raw_payload
FROM fact_predictions fp
JOIN raw_happiness_events r ON fp.raw_event_id = r.raw_event_id
JOIN dim_country dc ON fp.country_id = dc.country_id
JOIN dim_date dd ON fp.date_id = dd.date_id
ORDER BY fp.prediction_id DESC
LIMIT 20;
