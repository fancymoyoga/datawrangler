-- Silver: validated trips with derived duration, speed, and date columns.
CREATE OR REFRESH STREAMING TABLE silver_trips (
  CONSTRAINT valid_timestamps EXPECT (tpep_dropoff_datetime > tpep_pickup_datetime) ON VIOLATION DROP ROW,
  CONSTRAINT positive_distance EXPECT (trip_distance > 0) ON VIOLATION DROP ROW,
  CONSTRAINT positive_fare EXPECT (fare_amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT known_pickup_zip EXPECT (pickup_zip IS NOT NULL),
  CONSTRAINT reasonable_duration EXPECT (trip_duration_minutes <= 240)
)
COMMENT 'Cleaned NYC taxi trips with derived metrics'
CLUSTER BY (pickup_date, pickup_zip)
AS SELECT
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
  hour(tpep_pickup_datetime) AS pickup_hour,
  trip_distance,
  fare_amount,
  pickup_zip,
  dropoff_zip,
  round((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60.0, 2) AS trip_duration_minutes,
  round(fare_amount / trip_distance, 2) AS fare_per_mile
FROM STREAM(bronze_trips);
