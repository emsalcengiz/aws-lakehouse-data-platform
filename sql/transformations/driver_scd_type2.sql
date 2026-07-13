-- SQL Dialect: Athena / Trino
-- Target Table Format: Apache Iceberg
--
-- Maintain the driver dimension from daily full-load snapshots
-- using a hybrid Slowly Changing Dimension strategy.
--
-- Attribute strategy:
--
-- driver_name    -> SCD Type 1
-- city_id        -> SCD Type 2
-- vehicle_type   -> SCD Type 2
-- rating         -> SCD Type 1
-- driver_status  -> SCD Type 2
--
-- The source provides daily full-load snapshots rather than CDC.
--
-- Changes are derived by comparing the incoming snapshot with
-- the current dimension version.
--
-- The processing logic is designed to be idempotent.
--
-- Inferred driver members are resolved in place when the
-- authoritative driver snapshot arrives. The existing surrogate
-- key is preserved to prevent broken fact-to-dimension relationships.


-- ============================================================
-- STEP 1: CREATE DRIVER DIMENSION
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.dim_driver (
    driver_sk VARCHAR,
    driver_id BIGINT,
    driver_name VARCHAR,
    city_id INTEGER,
    vehicle_type VARCHAR,
    rating DECIMAL(3, 2),
    driver_status VARCHAR,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN,
    scd2_hash VARCHAR
)
LOCATION 's3://<YOUR-BUCKET>/silver/dim_driver/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);


-- ============================================================
-- STEP 2: DETECT SNAPSHOT CHANGES
-- ============================================================

CREATE OR REPLACE VIEW staging.driver_snapshot_changes AS

WITH snapshot_with_hash AS (

    SELECT
        driver_id,
        driver_name,
        city_id,
        vehicle_type,
        rating,
        driver_status,
        CAST(snapshot_date AS DATE) AS snapshot_date,

        TO_HEX(
            MD5(
                TO_UTF8(
                    CONCAT(
                        COALESCE(CAST(city_id AS VARCHAR), '<NULL>'),
                        '||',
                        COALESCE(vehicle_type, '<NULL>'),
                        '||',
                        COALESCE(driver_status, '<NULL>')
                    )
                )
            )
        ) AS scd2_hash

    FROM staging.drivers_snapshot
),


current_dimension AS (

    SELECT
        driver_id,
        driver_name,
        rating,
        driver_status,
        scd2_hash

    FROM silver.dim_driver

    WHERE is_current = TRUE
)


SELECT
    source.*,

    CASE

        WHEN target.driver_id IS NULL
            THEN 'NEW'

        WHEN target.driver_status = 'inferred'
            THEN 'INFERRED_RESOLUTION'

        WHEN source.scd2_hash IS DISTINCT FROM target.scd2_hash
            THEN 'SCD2_CHANGE'

        WHEN source.driver_name IS DISTINCT FROM target.driver_name
          OR source.rating IS DISTINCT FROM target.rating
            THEN 'SCD1_CHANGE'

        ELSE 'UNCHANGED'

    END AS change_type

FROM snapshot_with_hash AS source

LEFT JOIN current_dimension AS target
    ON source.driver_id = target.driver_id;


-- ============================================================
-- STEP 3: RESOLVE INFERRED DRIVER MEMBERS
-- ============================================================
--
-- When the authoritative driver snapshot arrives for an inferred
-- member, the placeholder record is completed in place.
--
-- The existing driver_sk is intentionally preserved.
--
-- Previously loaded facts therefore continue referencing the same
-- dimension surrogate key.

MERGE INTO silver.dim_driver AS target

USING staging.driver_snapshot_changes AS source

ON target.driver_id = source.driver_id
AND target.is_current = TRUE

WHEN MATCHED
AND source.change_type = 'INFERRED_RESOLUTION'

THEN UPDATE SET

    driver_name = source.driver_name,
    city_id = source.city_id,
    vehicle_type = source.vehicle_type,
    rating = source.rating,
    driver_status = source.driver_status,
    scd2_hash = source.scd2_hash;


-- ============================================================
-- STEP 4: EXPIRE CURRENT SCD TYPE 2 RECORDS
-- ============================================================
--
-- Historically meaningful attribute changes create a new
-- dimension version.
--
-- The current record is first expired before the new version
-- is inserted.

MERGE INTO silver.dim_driver AS target

USING staging.driver_snapshot_changes AS source

ON target.driver_id = source.driver_id
AND target.is_current = TRUE

WHEN MATCHED
AND source.change_type = 'SCD2_CHANGE'

THEN UPDATE SET

    valid_to = DATE_ADD(
        'day',
        -1,
        source.snapshot_date
    ),

    is_current = FALSE;


-- ============================================================
-- STEP 5: INSERT NEW DRIVERS AND SCD TYPE 2 VERSIONS
-- ============================================================

INSERT INTO silver.dim_driver (
    driver_sk,
    driver_id,
    driver_name,
    city_id,
    vehicle_type,
    rating,
    driver_status,
    valid_from,
    valid_to,
    is_current,
    scd2_hash
)

SELECT

    TO_HEX(
        MD5(
            TO_UTF8(
                CONCAT(
                    CAST(source.driver_id AS VARCHAR),
                    '||',
                    CAST(source.snapshot_date AS VARCHAR)
                )
            )
        )
    ) AS driver_sk,

    source.driver_id,
    source.driver_name,
    source.city_id,
    source.vehicle_type,
    source.rating,
    source.driver_status,
    source.snapshot_date AS valid_from,
    DATE '9999-12-31' AS valid_to,
    TRUE AS is_current,
    source.scd2_hash

FROM staging.driver_snapshot_changes AS source

WHERE source.change_type IN (
    'NEW',
    'SCD2_CHANGE'
)

AND NOT EXISTS (

    SELECT 1

    FROM silver.dim_driver AS target

    WHERE target.driver_id = source.driver_id
      AND target.valid_from = source.snapshot_date
);


-- ============================================================
-- STEP 6: APPLY SCD TYPE 1 UPDATES
-- ============================================================
--
-- driver_name and rating maintain only their latest values.
--
-- These attributes do not create a new dimension version.

MERGE INTO silver.dim_driver AS target

USING staging.driver_snapshot_changes AS source

ON target.driver_id = source.driver_id
AND target.is_current = TRUE

WHEN MATCHED

AND (
       target.driver_name IS DISTINCT FROM source.driver_name
    OR target.rating IS DISTINCT FROM source.rating
)

THEN UPDATE SET

    driver_name = source.driver_name,
    rating = source.rating;


/*
Example 1: Vehicle type change

driver_id = 3002

2026-05-08
vehicle_type = economy

2026-05-09
vehicle_type = comfort


Result:

driver_id | vehicle_type | valid_from | valid_to   | is_current
----------|--------------|------------|------------|-----------
3002      | economy      | 2026-05-08 | 2026-05-08 | FALSE
3002      | comfort      | 2026-05-09 | 9999-12-31 | TRUE


---------------------------------------------------------------

Example 2: City change

driver_id = 3006

city_id

35 → 34

A new SCD Type 2 version is created because city_id affects
historical driver and trip analysis.


---------------------------------------------------------------

Example 3: Rating change

driver_id = 3009

rating

4.30 → 4.38

rating is excluded from scd2_hash.

No new dimension version is created.

The current record is updated using SCD Type 1.


---------------------------------------------------------------

Example 4: Driver status change

driver_id = 3011

active → suspended

The historical status transition is preserved using SCD Type 2.


---------------------------------------------------------------

Example 5: Idempotent rerun

The same daily snapshot is processed twice.

First run:

old SCD2 record is expired
new version is inserted

Second run:

current scd2_hash matches source
change_type = UNCHANGED

NOT EXISTS also prevents inserting the same:

driver_id + valid_from

combination twice.

Result:

same snapshot → same dimension state


---------------------------------------------------------------

Example 6: Inferred driver resolution

Day 1:

trip_id = 5200
driver_id = 3050

Driver 3050 does not yet exist in dim_driver.

An inferred member is created:

driver_sk = INFERRED_DRIVER_3050
driver_status = inferred


Day 2:

The authoritative driver snapshot arrives:

driver_id = 3050
driver_name = Example Driver
city_id = 34
vehicle_type = comfort
rating = 4.75
driver_status = active


The existing inferred dimension member is updated in place.

The driver_sk remains:

INFERRED_DRIVER_3050


Result:

Previously loaded facts continue referencing the same surrogate key.

No fact-to-dimension relationship is broken.


---------------------------------------------------------------

Design principle:

Early-arriving fact
        ↓
Create inferred dimension member
        ↓
Load fact using inferred driver_sk
        ↓
Authoritative snapshot arrives
        ↓
Resolve inferred member in place
        ↓
Preserve driver_sk
*/