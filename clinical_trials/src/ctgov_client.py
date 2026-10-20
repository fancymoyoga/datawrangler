"""ClinicalTrials.gov v2 search client and landing-envelope writer.

Stdlib only, so the same code runs in the Databricks ingest notebook
(src/01_ingest_api.py) and in the local fallback (scripts/fetch_and_upload.py).

Searches GET /studies with query.cond (e.g. "lung cancer"), follows nextPageToken, and lands
each study as its own envelope, so bronze/silver see the same shape as a single-study fetch.
"""

import json
import os
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone

API_URL = "https://clinicaltrials.gov/api/v2/studies"
USER_AGENT = "clinical-trials-lakehouse/1.0"
EXPECTED_API_VERSION = "2.0.5"
POLITE_DELAY_SECONDS = 1.0
MAX_RETRIES = 4
BACKOFF_BASE_SECONDS = 2.0
MAX_RETRY_AFTER_SECONDS = 120
TIMEOUT_SECONDS = 60
PAGE_SIZE = 100

# Silver and gold only read these, so result/derived/document sections stay out of bronze.
FIELDS = ("protocolSection", "hasResults")
# Order matters because max_studies decides which studies are kept.
SORT = ("@relevance",)

# Patterns from docs/ctg-oas-v2.yaml (GET /studies). The spec's fields pattern doesn't group its
# alternation, so per CLAUDE.md each item is fullmatched against the grouped form.
_FIELD_PATTERN = r"[a-zA-Z][a-zA-Z0-9\-. ]*|@query"
_SORT_PATTERN = r"^(([a-zA-Z][a-zA-Z0-9\-. ]*)|(@relevance))(:(asc|desc))?$"
_NCT_ID = re.compile(r"^NCT[0-9]{8}$")


class ApiUnreachable(Exception):
    """The API could not be reached at the network level (DNS, connect, timeout)."""


def search_params(condition):
    """First-page query parameters, validated locally against the spec before any request."""
    condition = (condition or "").strip()
    if not condition:
        # With no query and no filter the API returns every study; never do that by accident.
        raise ValueError("A search condition is required (e.g. 'lung cancer')")
    for field in FIELDS:
        if not re.fullmatch(_FIELD_PATTERN, field):
            raise ValueError(f"Invalid fields item: {field!r}")
    if not 1 <= len(SORT) <= 2 or not all(re.fullmatch(_SORT_PATTERN, s) for s in SORT):
        raise ValueError(f"Invalid sort: {SORT!r}")
    return {"query.cond": condition, "fields": "|".join(FIELDS), "sort": "|".join(SORT)}


def _is_retryable(status):
    return status == 429 or status >= 500


def _retry_after_seconds(headers):
    value = headers.get("Retry-After") if headers else None
    if value and value.strip().isdigit():
        return min(int(value), MAX_RETRY_AFTER_SECONDS)
    return None


def _backoff(attempt):
    return BACKOFF_BASE_SECONDS * 2**attempt + random.uniform(0, 1)


def get(url, log=print):
    """GET a URL. Returns (http_status, body_bytes).

    429 and 5xx are retried with exponential backoff (honouring Retry-After); the last
    response is returned if retries run out, so callers can still land it. Other HTTP
    errors (e.g. 400) are returned immediately. Network-level failures that persist
    through every retry raise ApiUnreachable.
    """
    request = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": USER_AGENT})
    for attempt in range(MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as e:
            status, body = e.code, e.read()
            if not _is_retryable(status) or attempt == MAX_RETRIES:
                return status, body
            wait = _retry_after_seconds(e.headers) or _backoff(attempt)
            reason = f"HTTP {status}"
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            if attempt == MAX_RETRIES:
                raise ApiUnreachable(f"{url}: {e}") from e
            wait = _backoff(attempt)
            reason = repr(e)
        log(f"{reason}; retry {attempt + 1}/{MAX_RETRIES} in {wait:.1f}s: {url}")
        time.sleep(wait)


def check_api_version(log=print):
    """Warn if the live API version differs from the spec this code was written against."""
    status, body = get(f"{API_URL.rsplit('/', 1)[0]}/version", log=log)
    version = json.loads(body).get("apiVersion") if status == 200 else None
    if version != EXPECTED_API_VERSION:
        log(f"WARNING: API version is {version!r}, expected {EXPECTED_API_VERSION}; "
            "the spec in docs/ctg-oas-v2.yaml may have drifted")
    return version


def new_run():
    """Return (ingest_id, ingested_at) shared by every file written in one run."""
    return str(uuid.uuid4()), datetime.now(timezone.utc)


def build_envelope(nct_id, url, status, payload, ingested_at, ingest_id, query=None):
    """Wrap one study (or an error body). bytes payloads are parsed as JSON, else kept as text."""
    if isinstance(payload, (bytes, bytearray)):
        try:
            payload = json.loads(payload)
        except ValueError:
            payload = payload.decode("utf-8", errors="replace")
    envelope = {
        "requested_nct_id": nct_id,
        "source_url": url,
        "http_status": status,
        "ingested_at": ingested_at.isoformat(),
        "ingest_id": ingest_id,
        "payload": payload,
    }
    if query is not None:
        envelope["query"] = query
    return envelope


def relative_landing_path(folder, ingested_at, name=None):
    """{folder}/{utc_timestamp}_{name or folder}.json"""
    stamp = ingested_at.strftime("%Y%m%dT%H%M%SZ")
    return f"{folder}/{stamp}_{name or folder}.json"


def write_envelope(landing_dir, envelope, ingested_at, folder=None, name=None):
    """Write the envelope under landing_dir and return its full path.

    The file goes in a folder named after the study's NCT ID (or `folder`). It's written
    under a leading-underscore name and renamed into place, so Auto Loader (which skips
    files starting with '_') never reads a partial file.
    """
    folder = folder or envelope["requested_nct_id"]
    path = os.path.join(landing_dir, relative_landing_path(folder, ingested_at, name))
    directory, filename = os.path.split(path)
    os.makedirs(directory, exist_ok=True)
    tmp_path = os.path.join(directory, f"_{filename}.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(envelope, f, ensure_ascii=False)
    os.replace(tmp_path, path)
    return path


def _study_nct_id(study):
    ident = (study.get("protocolSection") or {}).get("identificationModule") or {}
    return ident.get("nctId")


def fetch_search(condition, landing_dir, max_studies, log=print, page_size=PAGE_SIZE):
    """Search for studies matching `condition`, landing up to `max_studies` of them.

    Returns (summary, unreachable): summary has total_count, pages and the list of
    (nct_id, http_status, path) written; unreachable is the ApiUnreachable error if the API
    dropped off the network (the run stops there). A non-200 page is landed as one error
    envelope (folder QUERY_ERROR) so silver quarantines it, and pagination stops.
    """
    if max_studies < 1:
        raise ValueError("max_studies must be at least 1")
    params = search_params(condition)
    ingest_id, ingested_at = new_run()
    log(f"ingest_id={ingest_id} ingested_at={ingested_at.isoformat()} "
        f"query.cond={params['query.cond']!r} max_studies={max_studies}")

    written, seen, pages, total, token = [], set(), 0, None, None
    try:
        check_api_version(log=log)
        while True:
            # Later pages resend the first request's parameters exactly; only pageSize,
            # countTotal and pageToken may change.
            page_params = {**params, "pageSize": min(page_size, max_studies)}
            if token:
                page_params["pageToken"] = token
            else:
                page_params["countTotal"] = "true"
            url = f"{API_URL}?{urllib.parse.urlencode(page_params)}"
            if pages:
                time.sleep(POLITE_DELAY_SECONDS)
            status, body = get(url, log=log)
            pages += 1

            if status != 200:
                # 400/404 bodies are text/plain; log and land them for quarantine.
                log(f"page {pages}: HTTP {status}: {body[:500].decode('utf-8', errors='replace')}")
                env = build_envelope(None, url, status, body, ingested_at, ingest_id, params)
                path = write_envelope(landing_dir, env, ingested_at, folder="QUERY_ERROR", name=f"page{pages}")
                written.append((None, status, path))
                break

            data = json.loads(body)
            if total is None:
                total = data.get("totalCount")
                log(f"{total} studies match; landing up to {max_studies}")
            studies = data.get("studies") or []  # an empty page is not the end
            for i, study in enumerate(studies):
                if len(seen) >= max_studies:
                    break
                nct_id = _study_nct_id(study)
                if nct_id in seen:
                    continue
                safe = nct_id if nct_id and _NCT_ID.match(nct_id) else None
                env = build_envelope(nct_id, url, 200, study, ingested_at, ingest_id, params)
                path = write_envelope(landing_dir, env, ingested_at, folder=safe or "NO_NCT_ID",
                                      name=None if safe else f"page{pages}_{i}")
                seen.add(nct_id)
                written.append((nct_id, 200, path))
            log(f"page {pages}: {len(studies)} studies, {len(seen)} landed so far")

            token = data.get("nextPageToken")
            if not token or len(seen) >= max_studies:
                break
    except ApiUnreachable as e:
        return {"total_count": total, "pages": pages, "written": written}, e
    return {"total_count": total, "pages": pages, "written": written}, None
