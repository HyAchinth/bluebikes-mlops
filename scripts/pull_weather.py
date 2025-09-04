import os
from datetime import datetime
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytz


API = "https://api.openweathermap.org/data/3.0/onecall"


def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def main(out_root: str, tz_name: str = "America/New_York"):
    key = os.environ.get("OWM_API_KEY")
    lat = float(os.environ.get("CITY_LAT", 42.3601))
    lon = float(os.environ.get("CITY_LON", -71.0589))
    if not key:
        raise RuntimeError("OWM_API_KEY not set")

    params = {
        "lat": lat,
        "lon": lon,
        "exclude": "minutely,daily,alerts",
        "appid": key,
        "units": "metric",
    }
    resp = requests.get(API, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    tz = pytz.timezone(tz_name)
    now = datetime.now(tz)
    ymd = now.strftime("%Y/%m/%d")
    hour = now.strftime("%H")

    hourly = data.get("hourly", [])
    if not hourly:
        raise RuntimeError("No hourly data returned from OWM")

    df = pd.DataFrame(hourly)
    # Convert 'dt' (unix) to UTC and local timestamps
    df["ts_utc"] = pd.to_datetime(df["dt"], unit="s", utc=True)
    df["ts_local"] = df["ts_utc"].dt.tz_convert(tz)

    out_dir = os.path.join(out_root, "raw", "weather", ymd)
    _ensure_dir(out_dir)
    out_path = os.path.join(out_dir, f"{hour}_hourly.parquet")
    pq.write_table(pa.Table.from_pandas(df), out_path)

    print({"written": out_path, "rows": len(df)})


if __name__ == "__main__":
    main(out_root=os.path.join(os.getcwd(), "data"), tz_name=os.getenv("TZ", "America/New_York"))
