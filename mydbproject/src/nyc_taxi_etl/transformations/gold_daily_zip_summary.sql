-- Gold: daily trip metrics per pickup/dropoff zip pair.
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_zip_summary
COMMENT 'Daily NYC taxi trip metrics by pickup and dropoff zip'
CLUSTER BY (pickup_date, pickup_zip)
AS SELECT
  pickup_date,
  pickup_zip,
  dropoff_zip,
  COUNT(*) AS trip_count,
  ROUND(SUM(fare_amount), 2) AS total_fare,
  ROUND(AVG(fare_amount), 2) AS avg_fare,
  ROUND(SUM(trip_distance), 2) AS total_distance,
  ROUND(AVG(trip_distance), 2) AS avg_distance,
  ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_minutes
FROM silver_trips
GROUP BY pickup_date, pickup_zip, dropoff_zip;
