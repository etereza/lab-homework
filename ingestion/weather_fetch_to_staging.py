import os
import requests
import psycopg2
import time
import urllib3
from datetime import datetime, timedelta
from dotenv import load_dotenv

# вимикаємо SSL warnings (бо ми будемо verify=False)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# завантажити змінні з ..\.env
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# --- Postgres creds ---
PG_HOST = os.getenv("PG_HOST")
PG_PORT = int(os.getenv("PG_PORT"))
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_SSLMODE = os.getenv("PG_SSLMODE", "require")

# --- Геолокація міста ---
LAT = float(os.getenv("CITY_LAT"))
LON = float(os.getenv("CITY_LON"))

# --- період погоди ---
START_DATE = datetime.strptime(os.getenv("WEATHER_START_DATE"), "%Y-%m-%d")
END_DATE   = datetime.strptime(os.getenv("WEATHER_END_DATE"), "%Y-%m-%d")


def get_day_weather(lat, lon, day_dt):
    """
    тягнемо погодинні дані за конкретний день і агрегуємо їх до денних метрик
    """
    day_str = day_dt.strftime("%Y-%m-%d")
    next_day_str = (day_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    url = (
        "https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={day_str}&end_date={next_day_str}"
        "&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation"
        "&timezone=UTC"
    )

    # головна зміна: verify=False
    r = requests.get(url, timeout=30,verify=False)
    r.raise_for_status()
    data = r.json()
    hourly = data.get("hourly", {})

    temps = hourly.get("temperature_2m", [])
    hums  = hourly.get("relative_humidity_2m", [])
    winds = hourly.get("wind_speed_10m", [])
    precs = hourly.get("precipitation", [])

    def avg_safe(lst):
        vals = [v for v in lst if v is not None]
        return sum(vals)/len(vals) if vals else None

    avg_temp = avg_safe(temps)
    min_temp = min(temps) if temps else None
    max_temp = max(temps) if temps else None
    avg_hum  = avg_safe(hums)
    avg_wind = avg_safe(winds)
    total_prec = sum([p for p in precs if p is not None]) if precs else None

    return {
        "dt": day_str,
        "avg_temp_c": avg_temp,
        "min_temp_c": min_temp,
        "max_temp_c": max_temp,
        "humidity_avg": avg_hum,
        "wind_speed_avg_ms": avg_wind,
        "precipitation_mm": total_prec,
    }


def ensure_table_and_insert(rows):
    # підʼєднання до віддаленого postgres
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        sslmode=PG_SSLMODE,
    )
    cur = conn.cursor()

    # створюємо схему staging і таблицю, якщо їх ще нема
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE TABLE IF NOT EXISTS staging.weather_daily (
            dt date PRIMARY KEY,
            avg_temp_c double precision,
            min_temp_c double precision,
            max_temp_c double precision,
            humidity_avg double precision,
            wind_speed_avg_ms double precision,
            precipitation_mm double precision
        );
    """)

    # upsert кожного дня
    for r in rows:
        cur.execute("""
            INSERT INTO staging.weather_daily (
                dt,
                avg_temp_c,
                min_temp_c,
                max_temp_c,
                humidity_avg,
                wind_speed_avg_ms,
                precipitation_mm
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (dt) DO UPDATE SET
                avg_temp_c = EXCLUDED.avg_temp_c,
                min_temp_c = EXCLUDED.min_temp_c,
                max_temp_c = EXCLUDED.max_temp_c,
                humidity_avg = EXCLUDED.humidity_avg,
                wind_speed_avg_ms = EXCLUDED.wind_speed_avg_ms,
                precipitation_mm = EXCLUDED.precipitation_mm;
        """, (
            r["dt"],
            r["avg_temp_c"],
            r["min_temp_c"],
            r["max_temp_c"],
            r["humidity_avg"],
            r["wind_speed_avg_ms"],
            r["precipitation_mm"],
        ))

    conn.commit()
    cur.close()
    conn.close()


def main():
    all_days = []
    cur_day = START_DATE
    while cur_day <= END_DATE:
        print(f"[WEATHER] fetching {cur_day.date()} ...")
        day_metrics = get_day_weather(LAT, LON, cur_day)
        all_days.append(day_metrics)
        cur_day += timedelta(days=1)
        time.sleep(0.5)

    ensure_table_and_insert(all_days)
    print(f"[WEATHER] inserted {len(all_days)} rows into staging.weather_daily")


if __name__ == "__main__":
    main()
