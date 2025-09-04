import os
import json
from datetime import datetime
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytz


def _now_local(tz_name: str):
    return datetime.now(pytz.timezone(tz_name))


def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def main(station_status_url: str, station_info_url: str, out_root: str, tz_name: str = "America/New_York"):
    """Pull GBFS station_status and station_information and write raw+slim parquet.

    Outputs:
      - data/raw/gbfs/YYYY/MM/DD/HHMM_station_status.json
      - data/raw/gbfs/YYYY/MM/DD/HHMM_station_information.json
      - data/raw/gbfs/slim/date=YYYY-MM-DD/snap-HHMM.parquet
    """
    now = _now_local(tz_name)
    ymd = now.strftime("%Y/%m/%d")
    hhmm = now.strftime("%H%M")

    raw_dir = os.path.join(out_root, "raw", "gbfs", ymd)
    _ensure_dir(raw_dir)

    ss = requests.get(station_status_url, timeout=20)
    si = requests.get(station_info_url, timeout=20)
    ss.raise_for_status(); si.raise_for_status()

    ss_path = os.path.join(raw_dir, f"{hhmm}_station_status.json")
    si_path = os.path.join(raw_dir, f"{hhmm}_station_information.json")

    with open(ss_path, "w", encoding="utf-8") as f:
        json.dump(ss.json(), f)
    with open(si_path, "w", encoding="utf-8") as f:
        json.dump(si.json(), f)

    # Build slim snapshot parquet (join status with minimal fields)
    status = ss.json()["data"]["stations"]
    status_df = pd.DataFrame(status)
    status_df["ingest_ts"] = now.astimezone(pytz.UTC)

    # Keep a minimal set of columns; create if missing
    keep = [
        "station_id", "num_bikes_available", "num_docks_available", "last_reported",
        "is_installed", "is_renting", "is_returning", "ingest_ts"
    ]
    for k in keep:
        if k not in status_df.columns:
            status_df[k] = None
    status_df = status_df[keep]

    slim_dir = os.path.join(out_root, "raw", "gbfs", "slim", f"date={now.strftime('%Y-%m-%d')}")
    _ensure_dir(slim_dir)
    pq_path = os.path.join(slim_dir, f"snap-{hhmm}.parquet")
    table = pa.Table.from_pandas(status_df)
    pq.write_table(table, pq_path)

    print({"raw_dir": raw_dir, "slim_file": pq_path})


if __name__ == "__main__":
    # Allow manual run for quick smoke test using env vars
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--status", dest="station_status_url", required=False)
    parser.add_argument("--info", dest="station_info_url", required=False)
    parser.add_argument("--out", dest="out_root", default=os.path.join(os.getcwd(), "data"))
    parser.add_argument("--tz", dest="tz_name", default=os.getenv("TZ", "America/New_York"))
    args = parser.parse_args()

    if not args.station_status_url or not args.station_info_url:
        # Resolve via GBFS_ROOT if not provided
        root = os.getenv("GBFS_ROOT", "https://gbfs.bluebikes.com/gbfs/gbfs.json")
        data = requests.get(root, timeout=20).json()
        feeds = data["data"].get("en", next(iter(data["data"].values())))
        fmap = {f["name"]: f["url"] for f in feeds["feeds"]}
        args.station_status_url = fmap["station_status"]
        args.station_info_url = fmap["station_information"]

    main(args.station_status_url, args.station_info_url, args.out_root, args.tz_name)
