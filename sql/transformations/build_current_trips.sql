-- SQL Dialect: Athena / Trino
-- Target Table Format: Apache Iceberg
--
-- Incrementally maintain the current state of each trip from the
-- append-only CDC event log.
--
-- Processing strategy:
--   1. Identify trip IDs affected inside the late-data watermark window.
--   2. Re-read the complete event history only for affected trips.
--   3. Deduplicate exact CDC event replays using event_id.
--   4. Deduplicate repeated logical business events.
--   5. Apply lifecycle-aware status precedence.
--   6. Rebuild the current state of affected trips.
--   7. MERGE the result into the Iceberg current-trip table.
--
-- Processing date used in the case:
--   2026-05-09
--
-- Late-data lookback window:
--   2 days
--
-- The lookback value is an explicit processing assumption and should
-- be tuned using observed source lateness distribution in production.


-- ============================================================
-- STEP 1: CREATE CURRENT TRIP ICEBERG TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.current_trips (
    trip_id BIGINT,
    rider_id BIGINT,
    driver_id BIGINT,
    city_id INTEGER,
    current_status VARCHAR,
    fare_amount DECIMAL(10, 2),
    surge_multiplier DECIMAL(3, 2),
    distance_km DECIMAL(8, 2),
    pickup_lat DECIMAL(9, 6),
    pickup_lng DECIMAL(9, 6),
    dropoff_lat DECIMAL(9, 6),
    dropoff_lng DECIMAL(9, 6),
    vehicle_type VARCHAR,
    promo_code_id BIGINT,
    payment_method VARCHAR,
    trip_date DATE,
    latest_event_id BIGINT,
    latest_event_ts TIMESTAMP,
    last_ingested_at TIMESTAMP
)
LOCATION 's3://<YOUR-BUCKET>/silver/current_trips/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);


-- ============================================================
-- STEP 2: REBUILD AND MERGE AFFECTED TRIPS
-- ============================================================

MERGE INTO silver.current_trips AS target

USING (

    WITH processing_parameters AS (

        SELECT
            DATE '2026-05-09' AS processing_date,
            2 AS late_data_lookback_days
    ),


    affected_trip_ids AS (

        -- ========================================================
        -- IDENTIFY TRIPS TO REPROCESS
        -- ========================================================
        --
        -- ingested_at is used for incremental ingestion detection.
        --
        -- A lookback window allows late-arriving events to reopen
        -- previously processed trips.

        SELECT DISTINCT
            events.trip_id

        FROM raw_trips_events AS events

        CROSS JOIN processing_parameters AS parameters

        WHERE CAST(events.ingested_at AS DATE)
              BETWEEN DATE_ADD(
                  'day',
                  -parameters.late_data_lookback_days,
                  parameters.processing_date
              )
              AND parameters.processing_date
    ),


    event_id_deduplication AS (

        -- ========================================================
        -- DEDUPLICATE EXACT CDC EVENT REPLAYS
        -- ========================================================

        SELECT
            event_id,
            trip_id,
            rider_id,
            driver_id,
            city_id,
            status,
            fare_amount,
            surge_multiplier,
            distance_km,
            pickup_lat,
            pickup_lng,
            dropoff_lat,
            dropoff_lng,
            vehicle_type,
            promo_code_id,
            event_ts,
            ingested_at,
            payment_method

        FROM (

            SELECT
                events.*,

                ROW_NUMBER() OVER (
                    PARTITION BY events.event_id
                    ORDER BY events.ingested_at DESC
                ) AS event_row_number

            FROM raw_trips_events AS events

            INNER JOIN affected_trip_ids AS affected
                ON events.trip_id = affected.trip_id
        )

        WHERE event_row_number = 1
    ),


    business_event_deduplication AS (

        -- ========================================================
        -- DEDUPLICATE LOGICAL BUSINESS EVENTS
        -- ========================================================
        --
        -- Sample data demonstrates that the same logical event may
        -- arrive with different event_id values.
        --
        -- Observed duplicate signature:
        --
        --   trip_id
        --   status
        --   event_ts
        --
        -- In production, a source-generated stable idempotency key
        -- would be preferred and should be defined in the data contract.

        SELECT
            event_id,
            trip_id,
            rider_id,
            driver_id,
            city_id,
            status,
            fare_amount,
            surge_multiplier,
            distance_km,
            pickup_lat,
            pickup_lng,
            dropoff_lat,
            dropoff_lng,
            vehicle_type,
            promo_code_id,
            event_ts,
            ingested_at,
            payment_method

        FROM (

            SELECT
                *,

                ROW_NUMBER() OVER (
                    PARTITION BY
                        trip_id,
                        status,
                        event_ts

                    ORDER BY
                        ingested_at DESC,
                        event_id DESC
                ) AS business_event_row_number

            FROM event_id_deduplication
        )

        WHERE business_event_row_number = 1
    ),


    trip_event_history AS (

        -- ========================================================
        -- ASSIGN LIFECYCLE PRECEDENCE
        -- ========================================================

        SELECT
            *,

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

        FROM business_event_deduplication
    ),


    trip_requested_dates AS (

        -- ========================================================
        -- DERIVE TRIP ATTRIBUTION DATE
        -- ========================================================
        --
        -- Trips are attributed to the requested event date.
        --
        -- A trip that completes after midnight remains associated
        -- with the date on which it was requested.

        SELECT
            trip_id,

            CAST(
                MIN(event_ts)
                    FILTER (WHERE status = 'requested')
                AS DATE
            ) AS trip_date

        FROM trip_event_history

        GROUP BY trip_id
    ),


    ranked_trip_events AS (

        -- ========================================================
        -- DERIVE CURRENT TRIP STATE
        -- ========================================================
        --
        -- event_ts alone is not sufficient.
        --
        -- Example:
        --
        -- completed
        --     ↓
        -- picked_up
        --
        -- A later picked_up event must not regress the analytical
        -- current state from completed back to picked_up.

        SELECT
            *,

            ROW_NUMBER() OVER (
                PARTITION BY trip_id

                ORDER BY
                    status_precedence DESC,
                    event_ts DESC,
                    event_id DESC
            ) AS trip_event_rank

        FROM trip_event_history
    )


    SELECT
        ranked.trip_id,
        ranked.rider_id,
        ranked.driver_id,
        ranked.city_id,
        ranked.status AS current_status,
        ranked.fare_amount,
        ranked.surge_multiplier,
        ranked.distance_km,
        ranked.pickup_lat,
        ranked.pickup_lng,
        ranked.dropoff_lat,
        ranked.dropoff_lng,
        ranked.vehicle_type,
        ranked.promo_code_id,
        ranked.payment_method,
        dates.trip_date,
        ranked.event_id AS latest_event_id,
        ranked.event_ts AS latest_event_ts,
        ranked.ingested_at AS last_ingested_at

    FROM ranked_trip_events AS ranked

    LEFT JOIN trip_requested_dates AS dates
        ON ranked.trip_id = dates.trip_id

    WHERE ranked.trip_event_rank = 1

) AS source

ON target.trip_id = source.trip_id


WHEN MATCHED THEN

    UPDATE SET

        rider_id = source.rider_id,
        driver_id = source.driver_id,
        city_id = source.city_id,
        current_status = source.current_status,
        fare_amount = source.fare_amount,
        surge_multiplier = source.surge_multiplier,
        distance_km = source.distance_km,
        pickup_lat = source.pickup_lat,
        pickup_lng = source.pickup_lng,
        dropoff_lat = source.dropoff_lat,
        dropoff_lng = source.dropoff_lng,
        vehicle_type = source.vehicle_type,
        promo_code_id = source.promo_code_id,
        payment_method = source.payment_method,
        trip_date = source.trip_date,
        latest_event_id = source.latest_event_id,
        latest_event_ts = source.latest_event_ts,
        last_ingested_at = source.last_ingested_at


WHEN NOT MATCHED THEN

    INSERT (
        trip_id,
        rider_id,
        driver_id,
        city_id,
        current_status,
        fare_amount,
        surge_multiplier,
        distance_km,
        pickup_lat,
        pickup_lng,
        dropoff_lat,
        dropoff_lng,
        vehicle_type,
        promo_code_id,
        payment_method,
        trip_date,
        latest_event_id,
        latest_event_ts,
        last_ingested_at
    )

    VALUES (
        source.trip_id,
        source.rider_id,
        source.driver_id,
        source.city_id,
        source.current_status,
        source.fare_amount,
        source.surge_multiplier,
        source.distance_km,
        source.pickup_lat,
        source.pickup_lng,
        source.dropoff_lat,
        source.dropoff_lng,
        source.vehicle_type,
        source.promo_code_id,
        source.payment_method,
        source.trip_date,
        source.latest_event_id,
        source.latest_event_ts,
        source.last_ingested_at
    );


/*
Example 1: Normal lifecycle

5001

requested
    ↓
driver_assigned
    ↓
picked_up
    ↓
completed

Result:

5001 → completed


---------------------------------------------------------------

Example 2: Late / out-of-order status regression

5104

requested
    ↓
driver_assigned
    ↓
picked_up
    ↓
completed
    ↓
picked_up

The final picked_up event has a later event_ts.

Latest timestamp only:

5104 → picked_up     WRONG

Lifecycle-aware ranking:

5104 → completed     CORRECT


---------------------------------------------------------------

Example 3: Business-level duplicate

5101

completed → event_id 700104
completed → event_id 700105

The event IDs are unique.

However:

trip_id = same
status = same
event_ts = same

The duplicate logical event is processed once.


---------------------------------------------------------------

Example 4: Trip spanning midnight

5105

requested:
2026-05-09 23:52

completed:
2026-05-10 00:14

trip_date:

2026-05-09

The completed event updates the same trip_id.


---------------------------------------------------------------

Example 5: Late-arriving event

A trip was processed yesterday.

A late event arrives today.

Because the trip_id appears inside the late-data watermark window:

affected_trip_ids
        ↓
complete trip history is re-read
        ↓
current state is rebuilt
        ↓
Iceberg MERGE updates the existing trip


---------------------------------------------------------------

Example 6: Idempotent rerun

The same processing date is executed twice.

Business events are deduplicated.

The same trip state is rebuilt.

MERGE matches using trip_id.

No duplicate current-trip row is inserted.

Result:

same input → same target state
*/