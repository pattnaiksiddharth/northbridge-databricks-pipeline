-- =====================================================================
-- NorthBridge Bank: Lakeflow Spark Declarative Pipeline (v2)
-- Pipeline settings: default catalog = northbridge, default schema = lakeflow
-- Layers: bronze (streaming tables) -> silver (materialized views + SCD2) -> gold (materialized views)
-- Table names are unqualified, so everything publishes to northbridge.lakeflow
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. BRONZE: raw, append-only, loaded incrementally with Auto Loader
-- ---------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE bronze_transactions
COMMENT 'Raw payment gateway transactions, append-only'
AS SELECT *, current_timestamp() AS _ingest_ts, _metadata.file_path AS _source_file
FROM STREAM read_files('/Volumes/northbridge/raw/landing/transactions', format => 'csv', header => true);

CREATE OR REFRESH STREAMING TABLE bronze_ledger
COMMENT 'Raw general ledger entries, append-only'
AS SELECT *, current_timestamp() AS _ingest_ts, _metadata.file_path AS _source_file
FROM STREAM read_files('/Volumes/northbridge/raw/landing/ledger', format => 'csv', header => true);

CREATE OR REFRESH STREAMING TABLE bronze_customers
COMMENT 'Raw core banking customer extracts, append-only'
AS SELECT *, current_timestamp() AS _ingest_ts, _metadata.file_path AS _source_file
FROM STREAM read_files('/Volumes/northbridge/raw/landing/customers', format => 'csv', header => true);


-- ---------------------------------------------------------------------
-- 2. SILVER: typed, validated, de-duplicated, with a quarantine table
-- ---------------------------------------------------------------------

-- Every bronze row gets typed values, the FIRST rule it breaks (if any),
-- and a duplicate rank computed among rows that passed all rules.
CREATE OR REFRESH MATERIALIZED VIEW silver_transactions_checked
COMMENT 'Typed bronze rows with reject_reason and duplicate rank'
AS
WITH typed AS (
  SELECT *,
    try_cast(step AS INT)                     AS step_i,
    try_cast(amount AS DECIMAL(18,2))         AS amount_d,
    try_cast(oldbalanceOrg AS DECIMAL(18,2))  AS obo_d,
    try_cast(newbalanceOrig AS DECIMAL(18,2)) AS nbo_d,
    try_cast(oldbalanceDest AS DECIMAL(18,2)) AS obd_d,
    try_cast(newbalanceDest AS DECIMAL(18,2)) AS nbd_d
  FROM bronze_transactions
),
flagged AS (
  SELECT *,
    CASE
      WHEN txn_id IS NULL THEN 'NULL_TXN_ID'
      WHEN amount_d IS NULL OR amount_d <= 0 THEN 'INVALID_AMOUNT'
      WHEN type IS NULL OR type NOT IN ('PAYMENT','TRANSFER','CASH_OUT','DEBIT','CASH_IN') THEN 'INVALID_TYPE'
      WHEN step_i IS NULL OR step_i < 1 OR step_i > 744 THEN 'INVALID_STEP'
      WHEN obo_d IS NULL OR nbo_d IS NULL OR obd_d IS NULL OR nbd_d IS NULL THEN 'NULL_BALANCE'
    END AS reject_reason
  FROM typed
)
SELECT *,
  row_number() OVER (
    PARTITION BY txn_id, (reject_reason IS NULL)
    ORDER BY _ingest_ts, _source_file
  ) AS rn
FROM flagged;


-- Clean transactions. The expectations are a second line of defence:
-- if any rule is ever violated, the pipeline update FAILS instead of loading bad data.
CREATE OR REFRESH MATERIALIZED VIEW silver_transactions (
  CONSTRAINT txn_id_not_null EXPECT (txn_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT amount_positive EXPECT (amount > 0) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_type EXPECT (txn_type IN ('PAYMENT','TRANSFER','CASH_OUT','DEBIT','CASH_IN')) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_step EXPECT (step BETWEEN 1 AND 744) ON VIOLATION FAIL UPDATE
)
COMMENT 'Clean, typed, de-duplicated transactions'
AS
SELECT
  txn_id,
  CAST(day AS INT)  AS day,
  step_i            AS step,
  type              AS txn_type,
  amount_d          AS amount,
  nameOrig          AS orig_id,
  obo_d             AS old_bal_orig,
  nbo_d             AS new_bal_orig,
  nameDest          AS dest_id,
  obd_d             AS old_bal_dest,
  nbd_d             AS new_bal_dest,
  try_cast(isFraud AS INT) AS is_fraud_label
FROM silver_transactions_checked
WHERE reject_reason IS NULL AND rn = 1;


-- Quarantine: failed rows (with the rule they broke) plus duplicates
CREATE OR REFRESH MATERIALIZED VIEW silver_transactions_rejected
COMMENT 'Rows that failed a quality rule, or were duplicates, with the reason'
AS
SELECT
  txn_id,
  CAST(day AS INT)                   AS day,
  CAST(step AS STRING)               AS step,
  type,
  CAST(amount AS STRING)             AS amount,
  nameOrig,
  CAST(oldbalanceOrg AS STRING)      AS oldbalanceOrg,
  CAST(newbalanceOrig AS STRING)     AS newbalanceOrig,
  nameDest,
  CAST(oldbalanceDest AS STRING)     AS oldbalanceDest,
  CAST(newbalanceDest AS STRING)     AS newbalanceDest,
  CAST(isFraud AS STRING)            AS isFraud,
  CAST(isFlaggedFraud AS STRING)     AS isFlaggedFraud,
  CASE WHEN reject_reason IS NOT NULL THEN reject_reason ELSE 'DUPLICATE_TXN_ID' END AS reject_reason,
  _ingest_ts,
  _source_file
FROM silver_transactions_checked
WHERE reject_reason IS NOT NULL OR rn > 1;


CREATE OR REFRESH MATERIALIZED VIEW silver_ledger (
  CONSTRAINT txn_id_not_null EXPECT (txn_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT amount_present EXPECT (ledger_amount IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Clean, typed, de-duplicated ledger entries'
AS
SELECT txn_id, account_id, txn_type, ledger_amount, day
FROM (
  SELECT txn_id, account_id, txn_type,
         try_cast(ledger_amount AS DECIMAL(18,2)) AS ledger_amount,
         CAST(day AS INT) AS day,
         row_number() OVER (PARTITION BY txn_id ORDER BY _ingest_ts) AS rn
  FROM bronze_ledger
  WHERE txn_id IS NOT NULL
    AND try_cast(ledger_amount AS DECIMAL(18,2)) IS NOT NULL
)
WHERE rn = 1;


-- ---------------------------------------------------------------------
-- 3. SILVER dimension: SCD Type 2 with AUTO CDC (replaces the manual MERGE)
--    The target gets __START_AT and __END_AT; the current version has __END_AT = NULL
-- ---------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE customers_clean
COMMENT 'Typed customer change records, ready for CDC'
AS
SELECT customer_id, full_name, phone, city, account_status,
       try_cast(updated_at AS TIMESTAMP) AS updated_at
FROM STREAM(bronze_customers)
WHERE customer_id IS NOT NULL
  AND try_cast(updated_at AS TIMESTAMP) IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE dim_customer;

CREATE FLOW dim_customer_flow AS AUTO CDC INTO dim_customer
FROM STREAM(customers_clean)
KEYS (customer_id)
SEQUENCE BY updated_at
STORED AS SCD TYPE 2;


-- ---------------------------------------------------------------------
-- 4. GOLD
-- ---------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_kpis
COMMENT 'Daily volume, value and rejected rate by transaction type'
AS
WITH s AS (
  SELECT day, txn_type, count(*) AS txn_count, sum(amount) AS total_amount, avg(amount) AS avg_amount
  FROM silver_transactions
  GROUP BY day, txn_type
),
r AS (
  SELECT day, coalesce(type, 'UNKNOWN') AS txn_type, count(*) AS rejected_count
  FROM silver_transactions_rejected
  GROUP BY day, coalesce(type, 'UNKNOWN')
)
SELECT coalesce(s.day, r.day) AS day,
       coalesce(s.txn_type, r.txn_type) AS txn_type,
       coalesce(s.txn_count, 0) AS txn_count,
       s.total_amount,
       s.avg_amount,
       coalesce(r.rejected_count, 0) AS rejected_count,
       round(coalesce(r.rejected_count, 0) / (coalesce(s.txn_count, 0) + coalesce(r.rejected_count, 0)), 6) AS rejected_rate
FROM s FULL OUTER JOIN r ON s.day = r.day AND s.txn_type = r.txn_type;


-- Tiered fraud flags: every drained transfer / cash-out, ranked HIGH / MEDIUM / LOW
CREATE OR REFRESH MATERIALIZED VIEW gold_fraud_flags
COMMENT 'Risk-tiered fraud alerts'
AS
WITH f AS (
  SELECT txn_id, day, txn_type, amount, orig_id, dest_id, is_fraud_label,
         (old_bal_orig > 0 AND new_bal_orig = 0) AS drained,
         (amount >= 200000)                       AS large_amt,
         (old_bal_dest = 0)                       AS dest_empty
  FROM silver_transactions
  WHERE txn_type IN ('TRANSFER', 'CASH_OUT')
),
r AS (
  SELECT *,
    filter(array(IF(drained,    'ACCOUNT_DRAINED', NULL),
                 IF(large_amt,  'LARGE_AMOUNT',    NULL),
                 IF(dest_empty, 'DEST_EMPTY',      NULL)), x -> x IS NOT NULL) AS reasons
  FROM f
  WHERE drained
)
SELECT txn_id, day, txn_type, amount, orig_id, dest_id, is_fraud_label,
       CASE WHEN large_amt AND dest_empty THEN 'HIGH'
            WHEN dest_empty THEN 'MEDIUM'
            ELSE 'LOW' END AS risk_tier,
       reasons,
       array_join(reasons, ',') AS reason_list
FROM r;


CREATE OR REFRESH MATERIALIZED VIEW gold_reconciliation
COMMENT 'Gateway vs ledger mismatches'
AS
SELECT coalesce(g.txn_id, l.txn_id) AS txn_id,
       coalesce(g.day, l.day)       AS day,
       g.amount                     AS gateway_amount,
       l.ledger_amount,
       l.ledger_amount - g.amount   AS amount_diff,
       CASE WHEN l.txn_id IS NULL THEN 'MISSING_IN_LEDGER'
            WHEN g.txn_id IS NULL THEN 'MISSING_IN_GATEWAY'
            ELSE 'AMOUNT_MISMATCH' END AS recon_status
FROM silver_transactions g
FULL OUTER JOIN silver_ledger l ON g.txn_id = l.txn_id
WHERE l.txn_id IS NULL OR g.txn_id IS NULL OR g.amount <> l.ledger_amount;


-- Point-in-time join: each transaction gets the customer version valid when it happened.
-- Assumes step 1 = 2024-01-01 00:00, matching the customer extracts.
CREATE OR REFRESH MATERIALIZED VIEW gold_fact_transactions (
  CONSTRAINT customer_matched EXPECT (orig_city IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Transactions with customer context and fraud tier'
AS
WITH t AS (
  SELECT *, timestampadd(HOUR, step - 1, TIMESTAMP'2024-01-01 00:00:00') AS txn_ts
  FROM silver_transactions
)
SELECT t.txn_id, t.day, t.step, t.txn_ts, t.txn_type, t.amount, t.orig_id,
       c.city AS orig_city, c.account_status AS orig_account_status,
       t.dest_id, t.old_bal_orig, t.new_bal_orig, t.old_bal_dest, t.new_bal_dest,
       t.is_fraud_label,
       f.txn_id IS NOT NULL AS is_flagged,
       f.reason_list        AS flag_reasons,
       f.risk_tier          AS risk_tier
FROM t
LEFT JOIN dim_customer c
  ON  t.orig_id = c.customer_id
  AND t.txn_ts >= c.`__START_AT`
  AND (c.`__END_AT` IS NULL OR t.txn_ts < c.`__END_AT`)
LEFT JOIN gold_fraud_flags f ON t.txn_id = f.txn_id;


CREATE OR REFRESH MATERIALIZED VIEW gold_customer_360
COMMENT 'One row per customer: profile, behaviour and risk'
AS
WITH cur AS (
  SELECT customer_id, city, account_status FROM dim_customer WHERE `__END_AT` IS NULL
),
hist AS (
  SELECT customer_id, count(*) AS versions FROM dim_customer GROUP BY customer_id
),
beh AS (
  SELECT orig_id AS customer_id,
         count(*) AS txn_count, sum(amount) AS total_sent, avg(amount) AS avg_sent,
         max(day) AS last_txn_day,
         sum(CASE WHEN is_flagged THEN 1 ELSE 0 END)           AS flagged_txns,
         sum(CASE WHEN risk_tier = 'HIGH' THEN 1 ELSE 0 END)   AS high_txns
  FROM gold_fact_transactions
  GROUP BY orig_id
)
SELECT c.customer_id, c.city, c.account_status, h.versions AS profile_versions,
       coalesce(b.txn_count, 0) AS txn_count, b.total_sent, b.avg_sent, b.last_txn_day,
       coalesce(b.flagged_txns, 0) AS flagged_txns,
       CASE WHEN coalesce(b.high_txns, 0) >= 1 THEN 'HIGH'
            WHEN coalesce(b.flagged_txns, 0) >= 1 THEN 'MEDIUM'
            ELSE 'LOW' END AS risk_level,
       CASE WHEN b.total_sent >= 500000 THEN 'HIGH_VALUE'
            WHEN b.total_sent >= 50000  THEN 'MID_VALUE'
            ELSE 'STANDARD' END AS value_segment
FROM cur c
LEFT JOIN hist h ON c.customer_id = h.customer_id
LEFT JOIN beh  b ON c.customer_id = b.customer_id;