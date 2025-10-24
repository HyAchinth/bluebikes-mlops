#!/usr/bin/env python3
"""
Merge Bluebikes trip history + Open-Meteo hourly weather → feature_store/.
"""

import pathlib, zipfile, pandas as pd, numpy as np, requests, holidays
from tqdm import tqdm

TRIPS_ROOT = pathlib.Path("data/raw/trips")
WEATHER_CSV = pathlib.Path("data/raw/weather/historical_boston_weather_data.csv")
OUT_ROOT = pathlib.Path("data/feature_store")

STATION_INFO_URL = "https://gbfs.bluebikes.com/gbfs/en/station_information.json"
US_HOLIDAYS = holidays.US(years=range(2020, 2031))


# ---------- helpers ----------
def load_station_info() -> pd.DataFrame:
    print("[INFO] Fetching station information…")
    info = requests.get(STATION_INFO_URL, timeout=30).json()
    stations = pd.DataFrame(info["data"]["stations"])
    return stations[["station_id", "capacity", "lat", "lon", "name"]]


def read_trips() -> pd.DataFrame:
    dfs = []
    files = sorted(TRIPS_ROOT.glob("*.zip"))
    if not files:
        raise FileNotFoundError(f"No trip ZIPs found in {TRIPS_ROOT}")

    print(f"[INFO] Reading {len(files)} monthly trip archives…")
    for f in tqdm(files, desc="Reading Bluebikes trips", unit="file"):
        with zipfile.ZipFile(f) as zf:
            csvf = [n for n in zf.namelist() if n.endswith(".csv")][0]
            df = pd.read_csv(zf.open(csvf))
            for col in ["started_at", "ended_at"]:
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce", format="mixed")
            dfs.append(df)

    trips = pd.concat(dfs, ignore_index=True).dropna(subset=["started_at", "ended_at"])
    print(f"[INFO] Combined {len(trips):,} trip rows.")
    return trips


def build_hourly_demand(trips: pd.DataFrame) -> pd.DataFrame:
    trips["ts_hour"] = trips["ended_at"].dt.floor("h")
    hourly = (
        trips.groupby(["end_station_id", "ts_hour"])
        .size()
        .reset_index(name="rides")
        .rename(columns={"end_station_id": "station_id", "ts_hour": "ts"})
    )
    print(f"[INFO] Built hourly demand table with {len(hourly):,} rows.")
    return hourly


def read_weather() -> pd.DataFrame:
    """Read the Open-Meteo hourly CSV and safely localize time zone."""
    if not WEATHER_CSV.exists():
        raise FileNotFoundError(f"Weather CSV not found at {WEATHER_CSV}")

    w = pd.read_csv(WEATHER_CSV, parse_dates=["time"])
    w = w.rename(
        columns={
            "time": "ts_local",
            "temperature_2m_°c": "temp_c",
            "precipitation_mm": "precip_mm",
            "wind_speed_10m_km/h": "wind_speed_kmh",
        }
    )

    # Robust DST-safe localization
    try:
        w["ts_local"] = w["ts_local"].dt.tz_localize(
            "America/New_York", ambiguous="infer", nonexistent="shift_forward"
        )
    except Exception as e:
        print(f"[WARN] DST localization issue ({e}); retrying with ambiguous=True …")
        w["ts_local"] = w["ts_local"].dt.tz_localize(
            "America/New_York", ambiguous=True, nonexistent="shift_forward"
        )

    # Derived weather features
    w["wind_speed"] = w["wind_speed_kmh"]
    w["precip_prob"] = np.where(w["precip_mm"] > 0, 1.0, 0.0)
    w["rain_1h"] = w["precip_mm"]

    return w[["ts_local", "temp_c", "wind_speed", "precip_prob", "rain_1h"]]



# ---------- main ----------
def main():
    stations = load_station_info()
    trips = read_trips()
    demand = build_hourly_demand(trips)
    weather = read_weather()

    print("[INFO] Expanding hourly grid…")
    full_idx = pd.MultiIndex.from_product(
        [
            stations["station_id"].unique(),
            pd.date_range(demand["ts"].min(), demand["ts"].max(), freq="h", tz="UTC"),
        ],
        names=["station_id", "ts"],
    )
    demand = demand.set_index(["station_id", "ts"]).reindex(full_idx, fill_value=0).reset_index()

    print("[INFO] Joining station metadata and weather…")
    df = demand.merge(stations, on="station_id", how="left")

    df["ts_local"] = df["ts"].dt.tz_convert("America/New_York")

    # Ensure same tz on weather side (defensive)
    if weather["ts_local"].dt.tz is None:
        weather["ts_local"] = weather["ts_local"].dt.tz_localize(
            "America/New_York", ambiguous="infer", nonexistent="shift_forward"
        )
    else:
        weather["ts_local"] = weather["ts_local"].dt.tz_convert("America/New_York")

    df = df.merge(weather, on="ts_local", how="left")

    df["hour"] = df["ts_local"].dt.hour
    df["dow"] = df["ts_local"].dt.dayofweek
    df["is_holiday"] = df["ts_local"].dt.date.isin(US_HOLIDAYS)

    df = df.rename(columns={"rides": "bikes"})
    df["bikes"] = df["bikes"].astype(float)
    df["docks"] = df["capacity"] - df["bikes"].clip(upper=df["capacity"])

    req_cols = [
        "station_id", "ts_local", "bikes", "docks",
        "temp_c", "wind_speed", "precip_prob", "rain_1h",
        "dow", "hour", "is_holiday", "capacity"
    ]
    df = df[req_cols]

    print("[INFO] Writing daily partitions to feature_store/…")
    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    for date, g in tqdm(df.groupby(df["ts_local"].dt.date),
                        desc="Writing partitions", unit="day"):
        part = OUT_ROOT / f"date={date}"
        part.mkdir(parents=True, exist_ok=True)
        g.to_parquet(part / "features.parquet", index=False)

    print(f"[DONE] Feature store built under {OUT_ROOT.resolve()}")

if __name__ == "__main__":
    main()
