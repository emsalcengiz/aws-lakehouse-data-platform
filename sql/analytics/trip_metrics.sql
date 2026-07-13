-- SQL Dialect: Athena / Trino
-- Target Table Format: Apache Iceberg
--
-- Build trip lifecycle metrics from the cleaned CDC event log.
--
-- Grain:
--   One row per trip_id.
--
-- Trip attribution rule:
--   A trip is attributed to the calendar date of its requested event.
--
-- This means a trip requested on 2026-05-09 and completed after
-- midnight remains attributed to 2026-05-09.
--
-- Lifecycle metrics are derived from milestone timestamps.


WITH deduplicated_business_events AS (

    -- ============================================================
    -- STEP 1: DEDUPLICATE BUSINESS EVENTS
    -- ============================================================

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
        vehicle_type,
        event_ts,
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
            ) AS event_rank

        FROM raw_trips_events

    )

    WHERE event_rank = 1
),


trip_milestones AS (

    -- ============================================================
    -- STEP 2: DERIVE LIFECYCLE MILESTONES
    -- ============================================================

    SELECT
        trip_id,

        MIN_BY(rider_id, event_ts) AS rider_id,
        MAX_BY(driver_id, event_ts)
            FILTER (WHERE driver_id IS NOT NULL) AS driver_id,

        MIN_BY(city_id, event_ts) AS city_id,
        MIN_BY(vehicle_type, event_ts) AS vehicle_type,

        MIN(event_ts)
            FILTER (WHERE status = 'requested')
            AS requested_at,

        MIN(event_ts)
            FILTER (WHERE status = 'driver_assigned')
            AS driver_assigned_at,

        MIN(event_ts)
            FILTER (WHERE status = 'picked_up')
            AS picked_up_at,

        MIN(event_ts)
            FILTER (WHERE status = 'completed')
            AS completed_at,

        MAX_BY(fare_amount, event_ts)
            FILTER (WHERE status = 'completed')
            AS fare_amount,

        MAX_BY(distance_km, event_ts)
            FILTER (WHERE status = 'completed')
            AS distance_km,

        MAX_BY(payment_method, event_ts)
            FILTER (WHERE status = 'completed')
            AS payment_method,

        MAX(surge_multiplier) AS surge_multiplier

    FROM deduplicated_business_events

    GROUP BY trip_id
),


trip_metrics AS (

    -- ============================================================
    -- STEP 3: CALCULATE LIFECYCLE METRICS
    -- ============================================================

    SELECT
        trip_id,
        rider_id,
        driver_id,
        city_id,
        vehicle_type,

        requested_at,
        driver_assigned_at,
        picked_up_at,
        completed_at,

        CAST(requested_at AS DATE) AS trip_date,

        DATE_DIFF(
            'second',
            requested_at,
            driver_assigned_at
        ) AS time_to_assign_seconds,

        DATE_DIFF(
            'second',
            driver_assigned_at,
            picked_up_at
        ) AS wait_to_pickup_seconds,

        DATE_DIFF(
            'second',
            picked_up_at,
            completed_at
        ) AS trip_duration_seconds,

        DATE_DIFF(
            'second',
            requested_at,
            completed_at
        ) AS total_trip_lifecycle_seconds,

        fare_amount,
        distance_km,
        surge_multiplier,
        payment_method,

        CASE
            WHEN completed_at IS NOT NULL
                THEN 'completed'

            WHEN picked_up_at IS NOT NULL
                THEN 'picked_up'

            WHEN driver_assigned_at IS NOT NULL
                THEN 'driver_assigned'

            ELSE 'requested'
        END AS lifecycle_status

    FROM trip_milestones
)


-- ============================================================
-- STEP 4: RETURN TRIP METRICS FOR THE PROCESSING DATE
-- ============================================================

SELECT
    trip_id,
    rider_id,
    driver_id,
    city_id,
    vehicle_type,
    trip_date,

    requested_at,
    driver_assigned_at,
    picked_up_at,
    completed_at,

    time_to_assign_seconds,
    wait_to_pickup_seconds,
    trip_duration_seconds,
    total_trip_lifecycle_seconds,

    fare_amount,
    distance_km,
    surge_multiplier,
    payment_method,
    lifecycle_status

FROM trip_metrics

WHERE trip_date = DATE '2026-05-09'

ORDER BY trip_id;


/*
Example 1: Normal completed trip

requested
    ↓
driver_assigned
    ↓
picked_up
    ↓
completed

Metrics:

time_to_assign_seconds
    = driver_assigned_at - requested_at

wait_to_pickup_seconds
    = picked_up_at - driver_assigned_at

trip_duration_seconds
    = completed_at - picked_up_at

total_trip_lifecycle_seconds
    = completed_at - requested_at


---------------------------------------------------------------

Example 2: Trip spanning midnight

trip_id = 5105

requested_at = 2026-05-09 23:52
completed_at = 2026-05-10 00:14

trip_date = 2026-05-09

The trip is attributed to the requested date.

The completed event still updates the same trip lifecycle record.


---------------------------------------------------------------

Example 3: Status regression

completed
    ↓
picked_up

Lifecycle status is not derived using the latest event timestamp.

Because completed_at exists:

lifecycle_status = completed

The later picked_up event does not regress the analytical trip state.


---------------------------------------------------------------

Example 4: Business-level duplicate

Two completed events have:

same trip_id
same status
same event_ts
different event_id

The business-event deduplication step keeps only one logical event.

Lifecycle metrics are therefore not double counted.
*/