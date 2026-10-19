-- Bronze: raw NYC taxi trips, incrementally ingested from the samples catalog.
CREATE OR REFRESH STREAMING TABLE bronze_trips
COMMENT 'Raw NYC taxi trips from samples.nyctaxi.trips'
AS SELECT
  *,
  current_timestamp() AS _ingested_at
FROM STREAM(samples.nyctaxi.trips);
