#!/usr/bin/env python3
"""
Download Bluebikes monthly trip archives to data/raw/trips/.

Example:
  python scripts/fetch_bluebikes_trips.py --start 2023-09-01 --end 2025-09-30
"""
from __future__ import annotations
import argparse, pathlib, sys, time
import pandas as pd
import requests

OUT_ROOT = pathlib.Path("data/raw/trips")

CANDIDATE_URLS = [
    # Historical pattern (most months):
    "https://s3.amazonaws.com/hubway-data/{ym}-bluebikes-tripdata.csv.zip",
    # Some months used a shorter name:
    "https://s3.amazonaws.com/hubway-data/{ym}-bluebikes-tripdata.zip",
]

def months_between(start: pd.Timestamp, end: pd.Timestamp):
    cur = pd.Timestamp(start.year, start.month, 1)
    stop = pd.Timestamp(end.year, end.month, 1)
    while cur <= stop:
        yield f"{cur.year}{cur.month:02d}"
        cur = (cur + pd.offsets.MonthBegin(1))

def try_download(url: str, dest: pathlib.Path, timeout=120) -> bool:
    r = requests.get(url, stream=True, timeout=timeout)
    if r.status_code == 200 and int(r.headers.get("Content-Length", "1")) > 5000:
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1<<20):
                if chunk:
                    f.write(chunk)
        return True
    return False

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--force", action="store_true", help="re-download if exists")
    ns = ap.parse_args()

    start = pd.to_datetime(ns.start).normalize()
    end = pd.to_datetime(ns.end).normalize()

    ok, fail = 0, 0
    for ym in months_between(start, end):
        dest = OUT_ROOT / f"{ym}-bluebikes-tripdata.csv.zip"
        if dest.exists() and not ns.force:
            print(f"[SKIP] {dest} exists")
            ok += 1
            continue
        success = False
        for pattern in CANDIDATE_URLS:
            url = pattern.format(ym=ym)
            print(f"[DL] {url}")
            try:
                if try_download(url, dest):
                    print(f"[OK]  -> {dest}")
                    success = True
                    break
            except requests.RequestException as e:
                print(f"[WARN] {e}")
                time.sleep(1)
        if success:
            ok += 1
        else:
            print(f"[FAIL] Could not download {ym} with known patterns")
            fail += 1

    print(f"[DONE] ok={ok} fail={fail}")
    return 0 if fail == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
