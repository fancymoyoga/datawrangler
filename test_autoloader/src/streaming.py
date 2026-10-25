"""Run availableNow streams to completion, restarting them when their input schema changes."""

# Restarts allowed per run for schema changes. Each restart picks up at least one new field.
MAX_SCHEMA_RESTARTS = 10

SCHEMA_CHANGE_MARKERS = (
    "UNKNOWN_FIELD_EXCEPTION",  # Auto Loader added new fields (addNewColumns)
    "UnknownFieldException",
    "DELTA_SCHEMA_CHANGED",  # a Delta source table gained columns
)


def is_schema_change(error: Exception) -> bool:
    """True when the stream stopped only because its input schema evolved."""
    message = str(error)
    return any(marker in message for marker in SCHEMA_CHANGE_MARKERS)


def run_until_done(start_query, label: str):
    """Start a stream with `start_query()` and wait for it, restarting after schema changes.

    `start_query` must build the DataFrame itself, so a restarted stream reads with the new schema.
    """
    for attempt in range(MAX_SCHEMA_RESTARTS + 1):
        query = start_query()
        try:
            query.awaitTermination()
        except Exception as error:
            if not is_schema_change(error) or attempt == MAX_SCHEMA_RESTARTS:
                raise
            print(f"{label}: schema changed, restarting ({attempt + 1}/{MAX_SCHEMA_RESTARTS})")
            continue
        progress = query.lastProgress
        print(f"{label}: {progress['numInputRows'] if progress else 0} rows in the last batch")
        return
