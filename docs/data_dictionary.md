# Case Study — Provided Datasets

You are given the following sample files. They are **deliberately small** so you
can inspect them by eye, but they are representative of the real, messy data on
the platform. Several questions ask you to work directly with these files.

> These samples stand in for tables that, in production, hold hundreds of
> millions of rows per day. Reason about the *shape* of the problems, not the
> volume.

---

## 1. `raw_trips_events.csv`
CDC-ingested, **append-only** event log. One row per change event; a single
`trip_id` therefore appears across multiple rows as its status evolves.

| Column | Type | Notes |
|---|---|---|
| `event_id` | BIGINT | CDC event sequence id (unique per row) |
| `trip_id` | BIGINT | Trip identifier (NOT unique — many events per trip) |
| `rider_id` | BIGINT | Rider identifier |
| `driver_id` | BIGINT | Driver identifier (empty before assignment) |
| `city_id` | INT | 34 = Istanbul, 6 = Ankara, 35 = Izmir |
| `status` | VARCHAR | requested, driver_assigned, picked_up, completed, cancelled_by_rider, cancelled_by_driver, no_driver_found |
| `fare_amount` | DECIMAL(10,2) | 0 until completion |
| `surge_multiplier` | DECIMAL(3,2) | Surge factor at request time |
| `distance_km` | DECIMAL(8,2) | Populated at completion |
| `pickup_lat` / `pickup_lng` | DECIMAL(9,6) | Pickup coordinates |
| `dropoff_lat` / `dropoff_lng` | DECIMAL(9,6) | Drop-off coordinates |
| `vehicle_type` | VARCHAR | economy, comfort, premium |
| `promo_code_id` | BIGINT | Promo used (nullable) |
| `event_ts` | TIMESTAMP | When the change occurred (upstream) |
| `ingested_at` | TIMESTAMP | When the row landed in the lake |
| `payment_method` | VARCHAR | **Note:** this column is not present in older data |

The **current processing date** referenced throughout the case is
**2026-05-09**.

---

## 2. `drivers_snapshot_2026-05-08.csv` and `drivers_snapshot_2026-05-09.csv`
Two consecutive **daily full-load reference dumps** of the driver reference
table. Comparing the two days is how you would detect what changed.

| Column | Type | Notes |
|---|---|---|
| `driver_id` | BIGINT | Natural key |
| `driver_name` | VARCHAR | |
| `city_id` | INT | Driver's home city |
| `vehicle_type` | VARCHAR | economy, comfort, premium |
| `rating` | DECIMAL(3,2) | Rolling driver rating |
| `driver_status` | VARCHAR | active, suspended |
| `snapshot_date` | DATE | The day this dump was taken |

---

## 3. `s3_inventory_sample.csv`
A sample of the S3 object inventory for the `raw/driver_locations/` prefix
(the GPS ping data). Useful for the storage / cost questions.

| Column | Type | Notes |
|---|---|---|
| `key` | VARCHAR | S3 object key (partitioned by `event_date`) |
| `size_bytes` | BIGINT | Object size |
| `storage_class` | VARCHAR | STANDARD, etc. |
| `last_modified` | TIMESTAMP | |
