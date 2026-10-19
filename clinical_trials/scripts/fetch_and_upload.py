"""Local fallback for src/01_ingest_api.py.

Searches the ClinicalTrials.gov v2 API on this machine, writes the same landing envelopes,
and uploads them to the landing volume with `databricks fs cp`. Use it when Databricks
compute cannot reach the API. Stdlib only.

    python scripts/fetch_and_upload.py --profile monicaenders \
        --landing-path /Volumes/workspace/dev_monica_enders_clinical_trials/landing/studies \
        --condition "lung cancer" --max-studies 100
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
from ctgov_client import fetch_search  # noqa: E402


def upload(local_dir, landing_path, profile):
    databricks = shutil.which("databricks")
    if not databricks:
        sys.exit("The databricks CLI is not on PATH; install it or pass --no-upload.")
    for folder, _, files in os.walk(local_dir):
        for name in sorted(files):
            local_file = os.path.join(folder, name)
            relative = os.path.relpath(local_file, local_dir).replace(os.sep, "/")
            target = f"dbfs:{landing_path}/{relative}"
            subprocess.run(
                [databricks, "fs", "cp", local_file, target, "--profile", profile],
                check=True,
            )
            print(f"uploaded {target}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--condition", default="lung cancer", help="condition to search (query.cond)")
    parser.add_argument("--max-studies", type=int, default=100, help="cap on studies landed")
    parser.add_argument("--landing-path", required=True, help="/Volumes/<catalog>/<schema>/<volume>/<folder>")
    parser.add_argument("--profile", help="Databricks CLI profile (required unless --no-upload)")
    parser.add_argument("--out-dir", help="keep the local files here (default: a temp dir, deleted after upload)")
    parser.add_argument("--no-upload", action="store_true", help="only fetch and write locally")
    args = parser.parse_args()

    landing_path = args.landing_path.rstrip("/")
    if not landing_path.startswith("/Volumes/"):
        parser.error("--landing-path must be a /Volumes/... path")
    if not args.no_upload and not args.profile:
        parser.error("--profile is required to upload")

    out_dir = args.out_dir or tempfile.mkdtemp(prefix="ctgov_landing_")
    try:
        result, unreachable = fetch_search(args.condition, out_dir, args.max_studies)
        if unreachable:
            print(f"ERROR: API unreachable from this machine too: {unreachable}", file=sys.stderr)
        if result["written"] and not args.no_upload:
            upload(out_dir, landing_path, args.profile)
        print(f"{len(result['written'])} studies landed of {result['total_count']} matching "
              f"in {result['pages']} page(s)")
        if args.no_upload or args.out_dir:
            print(f"local files in {out_dir}")
    finally:
        if not args.out_dir and not args.no_upload:
            shutil.rmtree(out_dir, ignore_errors=True)
    return 1 if unreachable else 0


if __name__ == "__main__":
    sys.exit(main())
