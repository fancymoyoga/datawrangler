"""Fetch one study from the ClinicalTrials.gov API v2 in FHIR JSON format.

Usage:
    python fetch_study_fhir.py NCT04280705
    python fetch_study_fhir.py NCT04280705 --out raw/NCT04280705.fhir.json

Calls GET /studies/{nctId}?format=fhir.json. The response is a FHIR Bundle
(content type application/fhir+json) whose entries include a ResearchStudy.
"""

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

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
            "The spec may have drifted; review ../docs/ctg-oas-v2.yaml before continuing."
        )
    return api_version


def validate_nct_id(nct_id: str) -> str:
    nct_id = nct_id.strip()
    if not re.fullmatch(NCT_ID_PATTERN, nct_id):
        raise ValueError(f"Invalid NCT ID {nct_id!r}; must match {NCT_ID_PATTERN}")
    return nct_id


def fetch_study_fhir(session: requests.Session, nct_id: str) -> dict:
    """Return the FHIR Bundle for one study.

    `fields` is deliberately not sent: the API rejects it with format=fhir.json.
    An alias NCT ID returns 301; requests follows the redirect automatically.
    """
    nct_id = validate_nct_id(nct_id)
    url = f"{BASE_URL}/studies/{nct_id}"
    response = session.get(
        url,
        params={"format": "fhir.json"},
        headers={"Accept": "application/fhir+json"},
        timeout=TIMEOUT_SECONDS,
    )

    if response.history:
        print(
            f"Note: {nct_id} redirected to {response.url} (alias NCT ID)",
            file=sys.stderr,
        )

    # 400 and 404 bodies are text/plain, not JSON.
    if response.status_code == 404:
        raise ApiError(f"404 Not Found for {nct_id}: {response.text.strip()}")
    if response.status_code == 400:
        raise ApiError(f"400 Bad Request for {nct_id}: {response.text.strip()}")
    response.raise_for_status()

    bundle = response.json()
    if bundle.get("resourceType") != "Bundle":
        raise ApiError(
            f"Expected a FHIR Bundle, got resourceType={bundle.get('resourceType')!r}"
        )
    return bundle


def summarize_bundle(bundle: dict) -> dict:
    """Pull a few headline values out of the Bundle without assuming any key exists."""
    resources = [
        entry.get("resource", {}) for entry in bundle.get("entry", []) if isinstance(entry, dict)
    ]
    research_study = next(
        (r for r in resources if r.get("resourceType") == "ResearchStudy"), {}
    )
    return {
        "bundle_type": bundle.get("type"),
        "bundle_timestamp": bundle.get("timestamp"),
        "entry_counts": dict(Counter(r.get("resourceType") for r in resources)),
        "research_study_id": research_study.get("id"),
        "title": research_study.get("title"),
        "status": research_study.get("status"),
        "contained_count": len(research_study.get("contained", [])),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("nct_id", help="NCT ID, e.g. NCT04280705")
    parser.add_argument(
        "--out",
        type=Path,
        help="Write the full Bundle here (use a git-ignored path such as raw/).",
    )
    args = parser.parse_args()

    try:
        nct_id = validate_nct_id(args.nct_id)
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 2

    with requests.Session() as session:
        try:
            check_api_version(session)
            bundle = fetch_study_fhir(session, nct_id)
        except (ApiError, requests.RequestException) as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(bundle, indent=2), encoding="utf-8")
        print(f"Wrote {args.out}", file=sys.stderr)

    print(json.dumps(summarize_bundle(bundle), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
