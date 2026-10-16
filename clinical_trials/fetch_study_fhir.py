"""Fetch one study from the ClinicalTrials.gov API v2 in FHIR JSON format.

Usage:
    python fetch_study_fhir.py NCT04280705
    python fetch_study_fhir.py NCT04280705 --out raw/NCT04280705.fhir.json
    python fetch_study_fhir.py NCT04280705 --csv raw/NCT04280705_contacts_locations.csv

Calls GET /studies/{nctId}?format=fhir.json. The response is a FHIR Bundle
(content type application/fhir+json) whose entries include a ResearchStudy.
fhir_contacts_locations_dataframe() turns it into a pandas DataFrame of
NCTId, BriefTitle, OfficialTitle and ContactsLocationsModule values.
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


# FHIR has no `fields` filter (the API returns 400), so these columns are pulled out of the
# full Bundle locally. They mirror NCTId, BriefTitle, OfficialTitle, ContactsLocationsModule.
# FHIR folds phoneExt into the phone value. site_status uses FHIR codes (e.g. "recruiting").
CONTACT_COLUMNS = [
    "nct_id", "brief_title", "official_title", "record_type",
    "facility", "site_status", "city", "state", "zip", "country", "lat", "lon",
    "contact_name", "contact_role", "contact_phone", "contact_email", "affiliation",
]

# associatedParty roles that are organisations or contacts, not overall officials.
_NON_OFFICIAL_ROLES = {"sponsor", "lead-sponsor", "collaborator", "recruitment-contact"}


def _coding(concept: dict | None) -> tuple[str | None, str | None]:
    """(code, display) of a CodeableConcept's first coding, falling back to its text."""
    concept = concept or {}
    first = next(iter(concept.get("coding", [])), {})
    return first.get("code"), first.get("display") or concept.get("text")


def _telecom(item: dict, system: str) -> str | None:
    values = [t.get("value") for t in item.get("telecom", []) if t.get("system") == system]
    return "; ".join(v for v in values if v) or None


def _name_text(item: dict) -> str | None:
    names = item.get("name")
    if isinstance(names, list):
        return next((n.get("text") for n in names if n.get("text")), None)
    return names


def fhir_contacts_locations_dataframe(bundle: dict) -> "pd.DataFrame":
    """One row per site contact, central contact, and overall official.

    record_type is "location", "central_contact", or "overall_official". A site with no
    contacts still gets one row (contact columns empty). Every key is treated as optional.
    """
    import pandas as pd  # imported here so the fetch functions work without pandas

    resources = [
        entry.get("resource", {}) for entry in bundle.get("entry", []) if isinstance(entry, dict)
    ]
    study = next((r for r in resources if r.get("resourceType") == "ResearchStudy"), {})
    contained = {c.get("id"): c for c in study.get("contained", [])}

    nct_id = next(
        (
            i.get("value")
            for i in study.get("identifier", [])
            if i.get("system") == "https://clinicaltrials.gov/nctid"
        ),
        None,
    )
    official_title = next(
        (
            label.get("value")
            for label in study.get("label", [])
            if _coding(label.get("type"))[0] == "official"
        ),
        None,
    )
    base = {"nct_id": nct_id, "brief_title": study.get("title"), "official_title": official_title}

    def resolve(ref: dict) -> dict:
        return contained.get((ref.get("reference") or "").lstrip("#"), {})

    rows = []
    for site_ref in study.get("site", []):
        location = resolve(site_ref)
        site_status = None
        # Sites with a recruitment status point at a site-level ResearchStudy, which holds
        # the status and in turn points at the Location. Sites without one point straight
        # at the Location.
        if location.get("resourceType") == "ResearchStudy":
            site_status = next(
                (_coding(p.get("state"))[0] for p in location.get("progressStatus", [])), None
            )
            location = resolve(next(iter(location.get("site", [])), {}))
        address = location.get("address", {})
        position = location.get("position", {})
        site = {
            **base,
            "record_type": "location",
            "facility": location.get("name"),
            "site_status": site_status,
            "city": address.get("city"),
            "state": address.get("state"),
            "zip": address.get("postalCode"),
            "country": address.get("country"),
            "lat": position.get("latitude"),
            "lon": position.get("longitude"),
        }
        contacts = location.get("contact", [])
        if not contacts:
            rows.append(site)
        for contact in contacts:
            rows.append({
                **site,
                "contact_name": _name_text(contact),
                "contact_role": _coding(contact.get("purpose"))[1],
                "contact_phone": _telecom(contact, "phone"),
                "contact_email": _telecom(contact, "email"),
            })

    for party in study.get("associatedParty", []):
        role_code, role_display = _coding(party.get("role"))
        ref = (party.get("party", {}).get("reference") or "").lstrip("#")
        if role_code == "recruitment-contact":
            practitioner = contained.get(ref, {})
            rows.append({
                **base,
                "record_type": "central_contact",
                "contact_name": party.get("name") or _name_text(practitioner),
                "contact_role": _coding((party.get("classifier") or [{}])[0])[1],
                "contact_phone": _telecom(practitioner, "phone"),
                "contact_email": _telecom(practitioner, "email"),
            })
        # The responsible-party PI points at a contained PractitionerRole; officials don't.
        elif role_code not in _NON_OFFICIAL_ROLES and "-ResponsibleParty-" not in ref:
            rows.append({
                **base,
                "record_type": "overall_official",
                "contact_name": party.get("name"),
                "contact_role": role_display,
                "affiliation": party.get("party", {}).get("display"),
            })

    return pd.DataFrame(rows, columns=CONTACT_COLUMNS)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("nct_id", help="NCT ID, e.g. NCT04280705")
    parser.add_argument(
        "--out",
        type=Path,
        help="Write the full Bundle here (use a git-ignored path such as raw/).",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        help="Write the contacts/locations DataFrame here as CSV (requires pandas).",
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

    if args.csv:
        frame = fhir_contacts_locations_dataframe(bundle)
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(args.csv, index=False)
        print(f"Wrote {args.csv} ({len(frame)} rows)", file=sys.stderr)
    return 0


def _running_in_notebook() -> bool:
    """True inside IPython/Jupyter/Databricks, where sys.argv holds the kernel's own args (-f ...)."""
    try:
        get_ipython  # noqa: F821  (injected into builtins by IPython kernels)
    except NameError:
        return False
    return True


if __name__ == "__main__" and not _running_in_notebook():
    sys.exit(main())
