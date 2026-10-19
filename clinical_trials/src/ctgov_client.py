"""ClinicalTrials.gov v2 client and landing-envelope writer.

Stdlib only, so the same code runs in the Databricks ingest notebook
(src/01_ingest_api.py) and in the local fallback (scripts/fetch_and_upload.py).
"""

import json
import os
import random
import re
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

API_URL = "https://clinicaltrials.gov/api/v2/studies/{nct_id}"
USER_AGENT = "clinical-trials-lakehouse/1.0"
POLITE_DELAY_SECONDS = 1.0
MAX_RETRIES = 4
BACKOFF_BASE_SECONDS = 2.0
MAX_RETRY_AFTER_SECONDS = 120
TIMEOUT_SECONDS = 30

_ID_SEPARATORS = re.compile(r"[,;\s]+")
# IDs become directory and file names, so only allow characters that are path-safe.
_SAFE_ID = re.compile(r"^[A-Z0-9]+$")


class ApiUnreachable(Exception):
    """The API could not be reached at the network level (DNS, connect, timeout)."""


def parse_nct_ids(raw):
    """Split a comma-, semicolon- or whitespace-separated list into unique, upper-cased IDs."""
    ids = []
    for token in _ID_SEPARATORS.split(raw or ""):
        token = token.strip().upper()
        if token and token not in ids:
            ids.append(token)
    unsafe = [i for i in ids if not _SAFE_ID.match(i)]
    if unsafe:
        raise ValueError(f"NCT IDs may only contain letters and digits: {unsafe}")
    if not ids:
        raise ValueError("No NCT IDs given")
    return ids


def _is_retryable(status):
    return status == 429 or status >= 500


def _retry_after_seconds(headers):
    value = headers.get("Retry-After") if headers else None
    if value and value.strip().isdigit():
        return min(int(value), MAX_RETRY_AFTER_SECONDS)
    return None


def _backoff(attempt):
    return BACKOFF_BASE_SECONDS * 2**attempt + random.uniform(0, 1)


def fetch_study(nct_id, log=print):
    """Fetch one study. Returns (url, http_status, body_bytes).

    429 and 5xx are retried with exponential backoff (honouring Retry-After); the last
    response is returned if retries run out, so callers can still land it. Other HTTP
    errors (e.g. 404) are returned immediately. Network-level failures that persist
    through every retry raise ApiUnreachable.
    """
    url = API_URL.format(nct_id=nct_id)
    request = urllib.request.Request(
        url, headers={"Accept": "application/json", "User-Agent": USER_AGENT}
    )
    for attempt in range(MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                return url, response.status, response.read()
        except urllib.error.HTTPError as e:
            status, body = e.code, e.read()
            if not _is_retryable(status) or attempt == MAX_RETRIES:
                return url, status, body
            wait = _retry_after_seconds(e.headers) or _backoff(attempt)
            reason = f"HTTP {status}"
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            if attempt == MAX_RETRIES:
                raise ApiUnreachable(f"{url}: {e}") from e
            wait = _backoff(attempt)
            reason = repr(e)
        log(f"{nct_id}: {reason}; retry {attempt + 1}/{MAX_RETRIES} in {wait:.1f}s")
        time.sleep(wait)


def new_run():
    """Return (ingest_id, ingested_at) shared by every file written in one run."""
    return str(uuid.uuid4()), datetime.now(timezone.utc)


def build_envelope(nct_id, url, status, body, ingested_at, ingest_id):
    """Wrap a response. The payload is the API's JSON as returned, or its raw text if not JSON."""
    try:
        payload = json.loads(body)
    except ValueError:
        payload = body.decode("utf-8", errors="replace")
    return {
        "requested_nct_id": nct_id,
        "source_url": url,
        "http_status": status,
        "ingested_at": ingested_at.isoformat(),
        "ingest_id": ingest_id,
        "payload": payload,
    }


def relative_landing_path(nct_id, ingested_at):
    """{NCT_ID}/{utc_timestamp}_{NCT_ID}.json"""
    stamp = ingested_at.strftime("%Y%m%dT%H%M%SZ")
    return f"{nct_id}/{stamp}_{nct_id}.json"


def write_envelope(landing_dir, envelope, ingested_at):
    """Write the envelope under landing_dir and return its full path.

    The file is written under a leading-underscore name and renamed into place, so
    Auto Loader (which skips files starting with '_') never reads a partial file.
    """
    path = os.path.join(landing_dir, relative_landing_path(envelope["requested_nct_id"], ingested_at))
    folder, name = os.path.split(path)
    os.makedirs(folder, exist_ok=True)
    tmp_path = os.path.join(folder, f"_{name}.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(envelope, f, ensure_ascii=False)
    os.replace(tmp_path, path)
    return path


def fetch_all(nct_ids, landing_dir, log=print):
    """Fetch and land each ID, pausing politely between calls.

    Returns (written, unreachable): the list of (nct_id, http_status, path) written, and
    the ApiUnreachable error if the API dropped off the network (remaining IDs are skipped).
    """
    ingest_id, ingested_at = new_run()
    log(f"ingest_id={ingest_id} ingested_at={ingested_at.isoformat()} ids={len(nct_ids)}")
    written = []
    for i, nct_id in enumerate(nct_ids):
        if i:
            time.sleep(POLITE_DELAY_SECONDS)
        try:
            url, status, body = fetch_study(nct_id, log=log)
        except ApiUnreachable as e:
            return written, e
        envelope = build_envelope(nct_id, url, status, body, ingested_at, ingest_id)
        path = write_envelope(landing_dir, envelope, ingested_at)
        log(f"{nct_id}: HTTP {status} -> {path}")
        written.append((nct_id, status, path))
    return written, None
