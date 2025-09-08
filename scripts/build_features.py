import os
import json
from datetime import date, datetime
import glob
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytz


def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def _localize(ts_utc: pd.Series, tz_name: str):
    return ts_utc.dt.tz_convert(pytz.timezone(tz_name))


def _target_dates(ds: str | None, tz_name: str):
    tz = pytz.timezone(tz_name)
    if ds:
        d = datetime.fromisoformat(ds).date()
    else:
        d = datetime.now(tz).date()
    return d


def bronze_clean(root: str, tz_name: str, ds: str | None = None):
    """Load raw -> write bronze tables partitioned by date.

    - GBFS slim: /data/raw/gbfs/slim/date=YYYY-MM-DD/*.parquet
    - Weather hourly: /data/raw/weather/YYYY/MM/DD/*_hourly.parquet
    Outputs bronze to /data/bronze/{gbfs,weather}/date=YYYY-MM-DD/*.parquet
    """
    d = _target_dates(ds, tz_name)
    ymd = d.strftime("%Y-%m-%d")
    # GBFS slim
    gbfs_glob = os.path.join(root, "raw", "gbfs", "slim", f"date={ymd}", "*.parquet")
    gbfs_files = sorted(glob.glob(gbfs_glob))
    if not gbfs_files:
        raise FileNotFoundError(f"No GBFS slim files for {ymd}")
    gbfs_df = pd.concat([pd.read_parquet(f) for f in gbfs_files], ignore_index=True)
    # Normalize types
    gbfs_df["last_reported"] = pd.to_datetime(gbfs_df["last_reported"], unit="s", errors="coerce", utc=True)
    gbfs_df["ingest_ts"] = pd.to_datetime(gbfs_df["ingest_ts"], utc=True)

    gbfs_out = os.path.join(root, "bronze", "gbfs", f"date={ymd}")
    _ensure_dir(gbfs_out)
    pq.write_table(pa.Table.from_pandas(gbfs_df), os.path.join(gbfs_out, "gbfs.parquet"))

    # Weather hourly (collect all hours under Y/M/D)
    w_glob = os.path.join(root, "raw", "weather", d.strftime("%Y/%m/%d"), "*_hourly.parquet")
    w_files = sorted(glob.glob(w_glob))
    if not w_files:
        raise FileNotFoundError(f"No weather hourly files for {ymd}")
    w_df = pd.concat([pd.read_parquet(f) for f in w_files], ignore_index=True)
    if "ts_utc" not in w_df:
        # rebuild timestamp if needed
        w_df["ts_utc"] = pd.to_datetime(w_df["dt"], unit="s", utc=True)
    w_out = os.path.join(root, "bronze", "weather", f"date={ymd}")
    _ensure_dir(w_out)
    pq.write_table(pa.Table.from_pandas(w_df), os.path.join(w_out, "weather.parquet"))

    print({"bronze_gbfs": gbfs_out, "bronze_weather": w_out})


def add_calendar(root: str, tz_name: str, ds: str | None = None):
    """Create calendar table with dow/hour/is_holiday for the day."""
    d = _target_dates(ds, tz_name)
    ymd = d.strftime("%Y-%m-%d")

    # Build hourly timeline for the day in local tz
    tz = pytz.timezone(tz_name)
    times = pd.date_range(start=pd.Timestamp(d, tz=tz), end=pd.Timestamp(d, tz=tz) + pd.Timedelta(days=1), freq="H", inclusive="left")
    cal = pd.DataFrame({"ts_local": times})
    cal["dow"] = cal["ts_local"].dt.dayofweek  # 0=Mon
    cal["hour"] = cal["ts_local"].dt.hour

    # Nager.Date holidays (US)
    year = d.year
    try:
        resp = requests.get(f"https://date.nager.at/api/v3/PublicHolidays/{year}/US", timeout=20)
        resp.raise_for_status()
        holidays = {pd.Timestamp(h["date"]).date() for h in resp.json()}
    except Exception:
        holidays = set()
    cal["is_holiday"] = cal["ts_local"].dt.date.isin(holidays)

    out = os.path.join(root, "bronze", "calendar", f"date={ymd}")
    _ensure_dir(out)
    pq.write_table(pa.Table.from_pandas(cal), os.path.join(out, "calendar.parquet"))
    print({"calendar": out, "rows": len(cal)})


def write_feature_store(root: str, tz_name: str, ds: str | None = None):
    """Join bronze tables into a daily feature set with required columns.

    Output: /data/feature_store/date=YYYY-MM-DD/features.parquet
    Columns: ts_utc, ts_local, station_id, num_bikes_available, num_docks_available,
             temp, wind_speed, precipitation (if present), dow, hour, is_holiday
    """
    d = _target_dates(ds, tz_name)
    ymd = d.strftime("%Y-%m-%d")
    tz = pytz.timezone(tz_name)

    # Load bronze
    gbfs_bronze = os.path.join(root, "bronze", "gbfs", f"date={ymd}", "gbfs.parquet")
    weather_bronze = os.path.join(root, "bronze", "weather", f"date={ymd}", "weather.parquet")
    cal_bronze = os.path.join(root, "bronze", "calendar", f"date={ymd}", "calendar.parquet")

    gbfs = pd.read_parquet(gbfs_bronze)
    weather = pd.read_parquet(weather_bronze)
    cal = pd.read_parquet(cal_bronze)
    print(weather.columns)

    # Align to hour for join: floor both to hourly buckets in UTC/local
    # Align to hour for join: floor both to hourly buckets in UTC/local
    gbfs["ts_utc_hour"] = gbfs["ingest_ts"].dt.floor("H")
    weather["ts_utc_hour"] = pd.to_datetime(weather["ts_utc"], utc=True).dt.floor("H")

    # Build weather column list dynamically (only pick what's present)
    weather_cols = ["ts_utc_hour"]
    for c in ["temp", "wind_speed", "pop", "rain", "rain.1h", "snow", "snow.1h", "precipitation"]:
        if c in weather.columns:
            weather_cols.append(c)

    # Join on hour (UTC)
    feat = gbfs.merge(weather[weather_cols], on="ts_utc_hour", how="left")

    # Localize for calendar join (keep tz-aware)
    feat["ts_local_hour"] = feat["ts_utc_hour"].dt.tz_convert(tz)
    # cal["ts_local"] is already tz-aware from add_calendar(); just floor
    cal["ts_local_hour"] = cal["ts_local"].dt.floor("H")
    feat = feat.merge(cal[["ts_local_hour", "dow", "hour", "is_holiday"]], on="ts_local_hour", how="left")

    # Final column selection & rename
    cols = {
        "ts_utc_hour": "ts_utc",
        "ts_local_hour": "ts_local",
        "station_id": "station_id",
        "num_bikes_available": "bikes",
        "num_docks_available": "docks",
        "temp": "temp_c",
        "wind_speed": "wind_speed",
    }

    # Precipitation: prefer measured mm (rain/snow), else use pop (probability)
    feat["precip_mm"] = pd.NA

    # Case A: nested dict in 'rain' with key '1h'
    if "rain" in feat.columns:
        def _extract_r1h(x):
            if isinstance(x, dict):
                return x.get("1h", None)
            return None
        r = feat["rain"].apply(_extract_r1h)
        feat.loc[r.notna(), "precip_mm"] = r

    # Case B: flattened 'rain.1h' numeric column
    if "rain.1h" in feat.columns:
        feat.loc[feat["rain.1h"].notna(), "precip_mm"] = feat["rain.1h"]

    # Optional: treat snow as precip if present (OpenWeather uses mm water equivalent)
    if "snow.1h" in feat.columns:
        # Fill only where precip_mm is still NA
        mask = feat["precip_mm"].isna() & feat["snow.1h"].notna()
        feat.loc[mask, "precip_mm"] = feat.loc[mask, "snow.1h"]

    # Fallback: if we still don't have measured precip but we do have probability,
    # you can choose to store probability instead (document that it's a probability 0..1).
    if "pop" in feat.columns:
        mask = feat["precip_mm"].isna() & feat["pop"].notna()
        feat.loc[mask, "precip_mm"] = feat.loc[mask, "pop"]


    final_cols = ["ts_utc", "ts_local", "station_id", "bikes", "docks", "temp_c", "wind_speed", "precip_mm", "dow", "hour", "is_holiday"]
    out_df = feat.rename(columns=cols)
    for c in final_cols:
        if c not in out_df.columns:
            out_df[c] = None
    out_df = out_df[final_cols].dropna(subset=["ts_utc", "station_id"]).reset_index(drop=True)

    out_dir = os.path.join(root, "feature_store", f"date={ymd}")
    _ensure_dir(out_dir)
    pq.write_table(pa.Table.from_pandas(out_df), os.path.join(out_dir, "features.parquet"))
    print({"feature_store": out_dir, "rows": len(out_df)})


if __name__ == "__main__":
    # Manual run (uses today by default)
    root = os.path.join(os.getcwd(), "data")
    bronze_clean(root, tz_name=os.getenv("TZ", "America/New_York"))
    add_calendar(root, tz_name=os.getenv("TZ", "America/New_York"))
    write_feature_store(root, tz_name=os.getenv("TZ", "America/New_York"))
