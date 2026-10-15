"""Retrieve one study from the ClinicalTrials.gov API v2 and load it into pandas DataFrames.

Usage:
    python study_dataframe.py NCT04280705
    python study_dataframe.py NCT04280705 --csv-dir raw/

Calls GET /studies/{nctId} (JSON format). Returns one DataFrame per table:

    study          one row, protocol fields flattened (lists left as Python lists)
    conditions     one row per condition
    arms           one row per arm group
    interventions  one row per intervention
    outcomes       one row per primary / secondary / other outcome
    locations      one row per site
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd
import requests

BASE_URL = "https://clinicaltrials.gov/api/v2"
EXPECTED_API_VERSION = "2.0.5"

# Copied exactly from the spec: /studies/{nctId} path parameter pattern.
NCT_ID_PATTERN = r"^[Nn][Cc][Tt]0*[1-9]\d{0,7}$"

TIMEOUT_SECONDS = 60


class ApiError(Exception):
    pass


def check_api_version(session: requests.Session) -> str:
    """Stop if the live API version differs from the spec this code was written against."""
    response = session.get(f"{BASE_URL}/version", timeout=TIMEOUT_SECONDS)
    response.raise_for_status()
    api_version = response.json().get("apiVersion")
    if api_version != EXPECTED_API_VERSION:
        raise ApiError(
            f"API version is {api_version!r}, expected {EXPECTED_API_VERSION!r}. "
            "The spec may have drifted; review ../../docs/ctg-oas-v2.yaml before continuing."
        )
    return api_version


def validate_nct_id(nct_id: str) -> str:
    nct_id = nct_id.strip()
    if not re.fullmatch(NCT_ID_PATTERN, nct_id):
        raise ValueError(f"Invalid NCT ID {nct_id!r}; must match {NCT_ID_PATTERN}")
    return nct_id


def fetch_study(session: requests.Session, nct_id: str) -> dict:
    """Return the Study JSON for one NCT ID. Alias IDs (301) are followed automatically."""
    nct_id = validate_nct_id(nct_id)
    response = session.get(
        f"{BASE_URL}/studies/{nct_id}",
        params={"format": "json"},
        timeout=TIMEOUT_SECONDS,
    )
    if response.history:
        print(f"Note: {nct_id} redirected to {response.url} (alias NCT ID)", file=sys.stderr)

    # 400 and 404 bodies are text/plain, not JSON.
    if response.status_code in (400, 404):
        raise ApiError(f"{response.status_code} for {nct_id}: {response.text.strip()}")
    response.raise_for_status()
    return response.json()


def study_to_dataframes(study: dict) -> dict[str, pd.DataFrame]:
    """Split one Study into tidy DataFrames keyed by nct_id. Every Study key is optional."""
    protocol = study.get("protocolSection", {})
    nct_id = protocol.get("identificationModule", {}).get("nctId")

    study_row = pd.json_normalize(protocol, sep=".")
    # Child lists get their own tables below; keep the one-row table to scalars and short lists.
    drop = [
        c for c in study_row.columns
        if c.startswith((
            "armsInterventionsModule.",
            "outcomesModule.",
            "contactsLocationsModule.locations",
        ))
    ]
    study_row = study_row.drop(columns=drop)
    study_row.insert(0, "nct_id", nct_id)
    study_row["hasResults"] = study.get("hasResults")

    def child_table(records: list, **extra) -> pd.DataFrame:
        frame = pd.json_normalize(records, sep=".") if records else pd.DataFrame()
        for i, (name, value) in enumerate(extra.items()):
            frame.insert(i, name, value)
        frame.insert(0, "nct_id", nct_id)
        return frame

    arms_module = protocol.get("armsInterventionsModule", {})
    outcomes_module = protocol.get("outcomesModule", {})
    outcomes = pd.concat(
        [
            child_table(outcomes_module.get(key, []), outcome_type=label)
            for key, label in (
                ("primaryOutcomes", "PRIMARY"),
                ("secondaryOutcomes", "SECONDARY"),
                ("otherOutcomes", "OTHER"),
            )
        ],
        ignore_index=True,
    )

    conditions = protocol.get("conditionsModule", {}).get("conditions", [])
    return {
        "study": study_row,
        "conditions": pd.DataFrame({"nct_id": nct_id, "condition": conditions}),
        "arms": child_table(arms_module.get("armGroups", [])),
        "interventions": child_table(arms_module.get("interventions", [])),
        "outcomes": outcomes,
        "locations": child_table(
            protocol.get("contactsLocationsModule", {}).get("locations", [])
        ),
    }


def get_study_dataframes(nct_id: str) -> dict[str, pd.DataFrame]:
    """Version-check the API, fetch one study, and return its DataFrames."""
    with requests.Session() as session:
        check_api_version(session)
        return study_to_dataframes(fetch_study(session, nct_id))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("nct_id", help="NCT ID, e.g. NCT04280705")
    parser.add_argument(
        "--csv-dir",
        type=Path,
        help="Write each DataFrame to <dir>/<nct_id>_<table>.csv (use a git-ignored path such as raw/).",
    )
    args = parser.parse_args()

    try:
        nct_id = validate_nct_id(args.nct_id)
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 2

    try:
        frames = get_study_dataframes(nct_id)
    except (ApiError, requests.RequestException) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    for name, frame in frames.items():
        print(f"{name:<14} {frame.shape[0]:>5} rows x {frame.shape[1]:>3} cols")

    if args.csv_dir:
        args.csv_dir.mkdir(parents=True, exist_ok=True)
        for name, frame in frames.items():
            frame.to_csv(args.csv_dir / f"{nct_id.upper()}_{name}.csv", index=False)
        print(f"Wrote CSVs to {args.csv_dir}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
