-- SQL Dialect: Athena / Trino
-- Target Table Format: Apache Iceberg
--
-- Handle early-arriving facts and late-arriving driver dimensions.
--
-- A trip fact must not be dropped when:
--
--   1. driver_id is NULL
--   2. driver_id exists in the event but is not yet available
--      in the driver dimension
--
-- The strategy uses:
--
--   UNKNOWN DRIVER
--       -> source driver_id is NULL
--
--   INFERRED DRIVER
--       -> source driver_id is known
--       -> dimension record has not arrived yet
--
-- This preserves the trip fact and prevents broken dimensional
-- relationships.


-- ============================================================
-- STEP 1: CREATE THE GLOBAL UNKNOWN DRIVER MEMBER
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
    'UNKNOWN_DRIVER' AS driver_sk,
    -1 AS driver_id,
    'Unknown Driver' AS driver_name,
    NULL AS city_id,
    NULL AS vehicle_type,
    NULL AS rating,
    'unknown' AS driver_status,
    DATE '1900-01-01' AS valid_from,
    DATE '9999-12-31' AS valid_to,
    TRUE AS is_current,
    NULL AS scd2_hash

WHERE NOT EXISTS (

    SELECT 1

    FROM silver.dim_driver

    WHERE driver_sk = 'UNKNOWN_DRIVER'
);


-- ============================================================
-- STEP 2: IDENTIFY MISSING DRIVER DIMENSION MEMBERS
-- ============================================================

CREATE OR REPLACE VIEW staging.missing_driver_members AS

SELECT DISTINCT
    trips.driver_id

FROM silver.current_trips AS trips

LEFT JOIN silver.dim_driver AS drivers

    ON trips.driver_id = drivers.driver_id
    AND drivers.is_current = TRUE

WHERE trips.driver_id IS NOT NULL
  AND drivers.driver_id IS NULL;


-- ============================================================
-- STEP 3: CREATE INFERRED DRIVER MEMBERS
-- ============================================================
--
-- A deterministic surrogate key is generated from driver_id.
--
-- Reprocessing the same missing driver does not create another
-- inferred member.
--
-- Business attributes remain NULL until the daily driver
-- snapshot provides the authoritative values.

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
    CONCAT(
        'INFERRED_DRIVER_',
        CAST(missing.driver_id AS VARCHAR)
    ) AS driver_sk,

    missing.driver_id,
    'Inferred Driver' AS driver_name,
    NULL AS city_id,
    NULL AS vehicle_type,
    NULL AS rating,
    'inferred' AS driver_status,
    DATE '1900-01-01' AS valid_from,
    DATE '9999-12-31' AS valid_to,
    TRUE AS is_current,
    NULL AS scd2_hash

FROM staging.missing_driver_members AS missing

WHERE NOT EXISTS (

    SELECT 1

    FROM silver.dim_driver AS drivers

    WHERE drivers.driver_id = missing.driver_id
      AND drivers.is_current = TRUE
);


-- ============================================================
-- STEP 4: RESOLVE FACT-TO-DIMENSION DRIVER KEY
-- ============================================================

CREATE OR REPLACE VIEW staging.trip_driver_key_resolution AS

SELECT
    trips.trip_id,
    trips.driver_id,

    CASE

        -- Source event does not contain a driver identifier.
        WHEN trips.driver_id IS NULL
            THEN 'UNKNOWN_DRIVER'

        -- Current dimension member exists.
        WHEN drivers.driver_sk IS NOT NULL
            THEN drivers.driver_sk

        -- Defensive fallback.
        ELSE 'UNKNOWN_DRIVER'

    END AS resolved_driver_sk

FROM silver.current_trips AS trips

LEFT JOIN silver.dim_driver AS drivers

    ON trips.driver_id = drivers.driver_id
    AND drivers.is_current = TRUE;


/*
Example 1: NULL driver_id

trip_id = 5106
status = completed
driver_id = NULL

The trip fact is NOT dropped.

Result:

trip_id | driver_id | resolved_driver_sk
--------|-----------|-------------------
5106    | NULL      | UNKNOWN_DRIVER


---------------------------------------------------------------

Example 2: Known driver_id but missing dimension record

trip_id = 5200
driver_id = 3050

Driver 3050 is not yet present in dim_driver.

An inferred member is created:

driver_sk     = INFERRED_DRIVER_3050
driver_id     = 3050
driver_status = inferred

The trip can therefore be loaded without waiting for the next
driver snapshot.


---------------------------------------------------------------

Example 3: Dimension arrives later

Day 1:

trip event
    ↓
driver_id = 3050
    ↓
driver dimension missing
    ↓
create inferred member


Day 2:

driver snapshot arrives
    ↓
driver_id = 3050
    ↓
authoritative driver attributes become available
    ↓
inferred member must be completed by the driver dimension process


---------------------------------------------------------------

Design principle:

Never drop a valid trip fact because a dimension record is late.

Early-arriving fact
        ↓
Unknown or inferred dimension member
        ↓
Fact remains loadable
        ↓
Dimension arrives later
        ↓
Inferred member is resolved


UNKNOWN DRIVER vs INFERRED DRIVER

UNKNOWN DRIVER
    driver_id itself is NULL.
    The relationship cannot currently be identified.

INFERRED DRIVER
    driver_id is known.
    The dimension record has not arrived yet.

These cases are intentionally modeled separately.
*/