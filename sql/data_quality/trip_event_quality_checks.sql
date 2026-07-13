-- SQL Dialect: Athena / Trino
--
-- Data quality assertions for the append-only trip CDC event log.
--
-- The assertions are aligned with the required quality categories:
--
--   1. Freshness
--   2. Volume
--   3. Schema
--   4. Referential Integrity
--   5. Distribution
--   6. Business Rule
--
-- Each assertion defines an explicit failure action:
--
--   BLOCK       -> stop downstream publication
--   ALERT       -> continue processing but notify the owning team
--   QUARANTINE  -> isolate invalid rows from the Silver layer
--
-- Processing date:
--   2026-05-09


-- ============================================================
-- DATA QUALITY ASSERTION SUMMARY
-- ============================================================

WITH processing_parameters AS (

    SELECT
        DATE '2026-05-09' AS processing_date
),


quality_assertions AS (

    -- ========================================================
    -- 1. FRESHNESS
    -- ========================================================
    --
    -- Detect events with suspicious ingestion timing.
    --
    -- ACTION: ALERT
    --
    -- A negative ingestion delay may indicate a timezone
    -- inconsistency between event_ts and ingested_at.
    --
    -- This should not be silently corrected until the source
    -- timezone contract is confirmed.

    SELECT
        'EVENT_INGESTION_TIME_CONSISTENCY' AS check_name,
        'FRESHNESS' AS category,
        'ALERT' AS failure_action,
        COUNT(*) AS failed_record_count

    FROM raw_trips_events

    WHERE ingested_at < event_ts


    UNION ALL


    -- ========================================================
    -- 2. VOLUME
    -- ========================================================
    --
    -- Detect a processing day with no ingested events.
    --
    -- ACTION: BLOCK
    --
    -- Zero rows may indicate an ingestion outage rather than
    -- legitimate business inactivity.

    SELECT
        'DAILY_EVENT_VOLUME' AS check_name,
        'VOLUME' AS category,
        'BLOCK' AS failure_action,

        CASE
            WHEN COUNT(*) = 0 THEN 1
            ELSE 0
        END AS failed_record_count

    FROM raw_trips_events AS events

    CROSS JOIN processing_parameters AS parameters

    WHERE CAST(events.ingested_at AS DATE)
          = parameters.processing_date


    UNION ALL


    -- ========================================================
    -- 3. SCHEMA
    -- ========================================================
    --
    -- Completed trips must contain a fare amount.
    --
    -- ACTION: QUARANTINE
    --
    -- The sample demonstrates a schema / semantic drift incident
    -- where fare_amount becomes NULL for a completed trip.

    SELECT
        'COMPLETED_TRIP_NULL_FARE' AS check_name,
        'SCHEMA' AS category,
        'QUARANTINE' AS failure_action,
        COUNT(*) AS failed_record_count

    FROM raw_trips_events

    WHERE status = 'completed'
      AND fare_amount IS NULL


    UNION ALL


    -- ========================================================
    -- 4. REFERENTIAL INTEGRITY
    -- ========================================================
    --
    -- Detect completed trips without a driver identifier.
    --
    -- ACTION: ALERT
    --
    -- The fact must not be dropped.
    --
    -- The downstream dimensional model should map the record to
    -- an inferred / unknown driver member until the dimension
    -- relationship can be resolved.

    SELECT
        'COMPLETED_TRIP_MISSING_DRIVER' AS check_name,
        'REFERENTIAL_INTEGRITY' AS category,
        'ALERT' AS failure_action,
        COUNT(*) AS failed_record_count

    FROM raw_trips_events

    WHERE status = 'completed'
      AND driver_id IS NULL


    UNION ALL


    -- ========================================================
    -- 5. DISTRIBUTION
    -- ========================================================
    --
    -- Detect fare values outside the accepted analytical range.
    --
    -- ACTION: QUARANTINE
    --
    -- The sample contains a negative fare amount.

    SELECT
        'FARE_OUT_OF_ACCEPTED_RANGE' AS check_name,
        'DISTRIBUTION' AS category,
        'QUARANTINE' AS failure_action,
        COUNT(*) AS failed_record_count

    FROM raw_trips_events

    WHERE fare_amount < 0
       OR fare_amount > 10000


    UNION ALL


    -- ========================================================
    -- 6. BUSINESS RULE
    -- ========================================================
    --
    -- Detect invalid pickup coordinates.
    --
    -- ACTION: QUARANTINE

    SELECT
        'INVALID_PICKUP_COORDINATES' AS check_name,
        'BUSINESS_RULE' AS category,
        'QUARANTINE' AS failure_action,
        COUNT(*) AS failed_record_count

    FROM raw_trips_events

    WHERE pickup_lat NOT BETWEEN -90 AND 90
       OR pickup_lng NOT BETWEEN -180 AND 180
       OR (pickup_lat = 0 AND pickup_lng = 0)


    UNION ALL


    -- ========================================================
    -- BUSINESS RULE: INVALID DROPOFF COORDINATES
    -- ========================================================

    SELECT
        'INVALID_DROPOFF_COORDINATES' AS check_name,
        'BUSINESS_RULE' AS category,
        'QUARANTINE' AS failure_action,
        COUNT(*) AS failed_record_count

    FROM raw_trips_events

    WHERE (
            dropoff_lat IS NOT NULL
            OR dropoff_lng IS NOT NULL
          )

      AND (
            dropoff_lat NOT BETWEEN -90 AND 90
            OR dropoff_lng NOT BETWEEN -180 AND 180
            OR (dropoff_lat = 0 AND dropoff_lng = 0)
          )


    UNION ALL


    -- ========================================================
    -- BUSINESS RULE: LOGICAL DUPLICATE EVENTS
    -- ========================================================
    --
    -- event_id uniqueness alone is insufficient.
    --
    -- The same logical event may be delivered more than once
    -- using a different event_id.
    --
    -- ACTION: ALERT
    --
    -- The processing pipeline deduplicates these records
    -- deterministically.

    SELECT
        'LOGICAL_DUPLICATE_EVENT' AS check_name,
        'BUSINESS_RULE' AS category,
        'ALERT' AS failure_action,
        COUNT(*) AS failed_record_count

    FROM (

        SELECT
            trip_id,
            status,
            event_ts

        FROM raw_trips_events

        GROUP BY
            trip_id,
            status,
            event_ts

        HAVING COUNT(*) > 1
    )


    UNION ALL


    -- ========================================================
    -- BUSINESS RULE: STATUS REGRESSION
    -- ========================================================
    --
    -- Detect a lower lifecycle state arriving after a higher
    -- lifecycle state.
    --
    -- ACTION: ALERT
    --
    -- The event remains in Bronze for auditability.
    -- Lifecycle-aware ranking prevents analytical state regression.

    SELECT
        'STATUS_REGRESSION' AS check_name,
        'BUSINESS_RULE' AS category,
        'ALERT' AS failure_action,
        COUNT(*) AS failed_record_count

    FROM (

        SELECT
            trip_id,
            event_ts,
            status_precedence,

            MAX(status_precedence) OVER (
                PARTITION BY trip_id
                ORDER BY event_ts, event_id
                ROWS BETWEEN UNBOUNDED PRECEDING
                         AND 1 PRECEDING
            ) AS previous_max_status_precedence

        FROM (

            SELECT
                event_id,
                trip_id,
                event_ts,

                CASE status
                    WHEN 'requested' THEN 1
                    WHEN 'driver_assigned' THEN 2
                    WHEN 'picked_up' THEN 3
                    WHEN 'completed' THEN 4
                    WHEN 'cancelled_by_rider' THEN 4
                    WHEN 'cancelled_by_driver' THEN 4
                    WHEN 'no_driver_found' THEN 4
                    ELSE 0
                END AS status_precedence

            FROM raw_trips_events
        )
    )

    WHERE status_precedence < previous_max_status_precedence
)


SELECT
    check_name,
    category,
    failure_action,
    failed_record_count,

    CASE
        WHEN failed_record_count = 0
            THEN 'PASSED'

        ELSE 'FAILED'
    END AS check_status

FROM quality_assertions

ORDER BY
    category,
    check_name;


/*
Example output:

| check_name                         | category              | action      | status |
|------------------------------------|-----------------------|-------------|--------|
| EVENT_INGESTION_TIME_CONSISTENCY   | FRESHNESS             | ALERT       | FAILED |
| DAILY_EVENT_VOLUME                 | VOLUME                | BLOCK       | PASSED |
| COMPLETED_TRIP_NULL_FARE           | SCHEMA                | QUARANTINE  | FAILED |
| COMPLETED_TRIP_MISSING_DRIVER      | REFERENTIAL_INTEGRITY | ALERT       | FAILED |
| FARE_OUT_OF_ACCEPTED_RANGE         | DISTRIBUTION          | QUARANTINE  | FAILED |
| INVALID_PICKUP_COORDINATES         | BUSINESS_RULE         | QUARANTINE  | FAILED |
| INVALID_DROPOFF_COORDINATES        | BUSINESS_RULE         | QUARANTINE  | PASSED |
| LOGICAL_DUPLICATE_EVENT            | BUSINESS_RULE         | ALERT       | FAILED |
| STATUS_REGRESSION                  | BUSINESS_RULE         | ALERT       | FAILED |


Failure handling:

BLOCK
    ↓
Stop Silver / Gold publication.
Raw Bronze data remains preserved.

ALERT
    ↓
Pipeline may continue.
Create an operational alert and investigate the source.

QUARANTINE
    ↓
Preserve the Bronze event.
Route the invalid row to a quarantine dataset.
Exclude the row from trusted Silver processing.


Important timezone observation:

Some events contain:

    ingested_at < event_ts

The inconsistency appears around neighbouring events of the same trip.

This may indicate that event_ts and ingested_at are produced using
different timezone assumptions.

The pipeline must not silently subtract or add a fixed timezone offset.

The correct engineering action is to confirm:

    - Is event_ts UTC or local time?
    - Is ingested_at UTC?
    - Does the producer include timezone metadata?
    - Did the source contract change?

After the source timezone semantics are confirmed, timestamps should
be normalized to UTC during Silver processing.
*/