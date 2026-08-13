import requests
import time
from datetime import datetime, timedelta
from supabase import create_client
import os

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase     = create_client(SUPABASE_URL, SUPABASE_KEY)

def upsert_with_retry(table, rows, on_conflict, batch_size=200):
    """Upsert i små batches med retry ved fejl."""
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        for attempt in range(3):
            try:
                supabase.table(table).upsert(batch, on_conflict=on_conflict).execute()
                break
            except Exception as e:
                print(f"  Upsert fejl forsøg {attempt+1}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
                else:
                    print(f"  Springer batch over efter 3 forsøg")

def collect_realtid_dk_hourly():
    print("Henter realtid data (ElectricityBalanceNonv)...")
    
    from_dt = (datetime.utcnow() - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M")
    
    for area in ["DK1", "DK2"]:
        offset = 0
        rows = []
        while True:
            for attempt in range(5):
                try:
                    r = requests.get(
                        "https://api.energidataservice.dk/dataset/ElectricityBalanceNonv",
                        params={
                            "filter": f'{{"PriceArea":"{area}"}}',
                            "start": from_dt,
                            "limit": 1000,
                            "offset": offset,
                            "sort": "HourUTC asc",
                        },
                        headers={
                            "User-Agent": "Mozilla/5.0 energi-dashboard/1.0"
                        },
                        timeout=30
                    )
                    if r.status_code == 429:
                        wait = 120 * (attempt + 1)
                        print(f"  Rate limit, venter {wait}s...")
                        time.sleep(wait)
                        continue
                    r.raise_for_status()
                    break
                except Exception as e:
                    print(f"  Fejl: {e}, venter 30s...")
                    time.sleep(30)
            
            records = r.json().get("records", [])
            if not records:
                break
            
            if offset == 0:
                print(f"  FELTER {area}:", list(records[0].keys()))
            
            for rec in records:
                dt_str = rec["HourDK"].replace("Z", "")
                for source, field in [
                    ("solar",       "SolarPower"),
                    ("offshore",    "OffshoreWindPower"),
                    ("onshore",     "OnshoreWindPower"),
                    ("consumption", "GrossConsumption"),
                ]:
                    rows.append({
                        "area":       area,
                        "source":     source,
                        "datetime":   dt_str,
                        "value_mwh":  round(rec.get(field, 0) or 0, 3),
                    })
            
            if len(records) < 1000:
                break
            offset += 1000
            time.sleep(2)
        
        if rows:
            upsert_with_retry("dk_production_hourly", rows, "area,source,datetime")
            print(f"  {area} realtid gemt ({len(rows) // 4} tidspunkter)")

def collect_realtime_prices():
    print("Henter realtid priser...")
    from_dt = (datetime.utcnow() - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M")

    for area in ["DK1", "DK2"]:
        # Elspotprices (historisk)
        elspot_rows = []
        for attempt in range(5):
            try:
                r = requests.get(
                    "https://api.energidataservice.dk/dataset/Elspotprices",
                    params={
                        "filter": f'{{"PriceArea":"{area}"}}',
                        "start": from_dt,
                        "limit": 1000,
                        "sort": "HourDK asc",
                    },
                    headers={"User-Agent": "Mozilla/5.0 energi-dashboard/1.0"},
                    timeout=30
                )
                if r.status_code == 429:
                    time.sleep(120 * (attempt + 1))
                    continue
                r.raise_for_status()
                break
            except Exception as e:
                print(f"  Fejl Elspot: {e}")
                time.sleep(30)

        for rec in r.json().get("records", []):
            dt_str = rec["HourDK"].replace("Z", "")
            elspot_rows.append({
                "area":      area,
                "datetime":  dt_str,
                "price_dkk": rec.get("SpotPriceDKK", 0) or 0,
            })

        if elspot_rows:
            upsert_with_retry("dk_prices_hourly", elspot_rows, "area,datetime")
            print(f"  {area} Elspot gemt ({len(elspot_rows)} timer)")

        # DayAheadPrices (nyeste - 15 min opløsning, aggreger til timer)
        dayahead_records = []
        for attempt in range(5):
            try:
                r = requests.get(
                    "https://api.energidataservice.dk/dataset/DayAheadPrices",
                    params={
                        "filter": f'{{"PriceArea":"{area}"}}',
                        "start": from_dt,
                        "limit": 1000,
                        "sort": "TimeDK asc",
                    },
                    headers={"User-Agent": "Mozilla/5.0 energi-dashboard/1.0"},
                    timeout=30
                )
                if r.status_code == 429:
                    time.sleep(120 * (attempt + 1))
                    continue
                r.raise_for_status()
                break
            except Exception as e:
                print(f"  Fejl DayAhead: {e}")
                time.sleep(30)

        dayahead_records = r.json().get("records", [])

        # Aggreger 15-min til timesgennemsnit
        hourly = {}
        for rec in dayahead_records:
            dt = datetime.fromisoformat(rec["TimeDK"].replace("Z", ""))
            dt_hour = dt.replace(minute=0, second=0, microsecond=0)
            key = dt_hour.strftime("%Y-%m-%dT%H:%M:%S")
            if key not in hourly:
                hourly[key] = []
            hourly[key].append(rec["DayAheadPriceDKK"])

        dayahead_rows = []
        for dt_str, prices in hourly.items():
            dayahead_rows.append({
                "area":      area,
                "datetime":  dt_str,
                "price_dkk": round(sum(prices) / len(prices), 6),
            })

        if dayahead_rows:
            upsert_with_retry("dk_prices_hourly", dayahead_rows, "area,datetime")
            print(f"  {area} DayAhead gemt ({len(dayahead_rows)} timer)")

        time.sleep(2)

def collect_temperature_realtime():
    print("Henter temperatur realtid (Open-Meteo)...")
    import json

    locations = {
        "Danmark": {"lat": 56.0, "lon": 10.0},
        "Norge":   {"lat": 60.5, "lon": 8.5},
        "Sverige": {"lat": 59.5, "lon": 15.0},
        "Tyskland":{"lat": 51.0, "lon": 10.0},
    }

    today = datetime.utcnow().date()
    date_from = (today - timedelta(days=3)).isoformat()
    date_to   = (today + timedelta(days=14)).isoformat()

    rows = []
    for country, coords in locations.items():
        for attempt in range(3):
            try:
                r = requests.get(
                    "https://api.open-meteo.com/v1/forecast",
                    params={
                        "latitude":  coords["lat"],
                        "longitude": coords["lon"],
                        "daily":     "temperature_2m_mean",
                        "start_date": date_from,
                        "end_date":   date_to,
                        "timezone":  "Europe/Copenhagen",
                    },
                    timeout=20
                )
                r.raise_for_status()
                data = r.json()
                dates  = data["daily"]["time"]
                temps  = data["daily"]["temperature_2m_mean"]
                today_str = today.isoformat()
                for date_str, temp in zip(dates, temps):
                    if temp is None:
                        continue
                    if date_str < today_str:
                        data_type = "historisk"
                    elif date_str == today_str:
                        data_type = "i dag"
                    else:
                        data_type = "forecast"
                    rows.append({
                        "country":       country,
                        "date":          date_str,
                        "temperature_c": round(temp, 2),
                        "data_type":     data_type,
                    })
                break
            except Exception as e:
                print(f"  Fejl temperatur {country}: {e}")
                time.sleep(10)
        time.sleep(1)

    if rows:
        upsert_with_retry("temperature_forecast", rows, "country,date", batch_size=200)
        print(f"  Temperaturdata gemt ({len(rows)} rækker)")

def collect_hydro_forecast_realtime():
    print("Henter nedbør realtid (Open-Meteo)...")

    locations = {
        "Norge":   {"lat": 61.5, "lon": 8.5},
        "Sverige": {"lat": 63.0, "lon": 14.0},
    }

    today = datetime.utcnow().date()
    date_from = (today - timedelta(days=14)).isoformat()
    date_to   = (today + timedelta(days=14)).isoformat()

    rows = []
    for country, coords in locations.items():
        # Historisk
        for attempt in range(3):
            try:
                r = requests.get(
                    "https://archive-api.open-meteo.com/v1/archive",
                    params={
                        "latitude":   coords["lat"],
                        "longitude":  coords["lon"],
                        "daily":      "precipitation_sum",
                        "start_date": date_from,
                        "end_date":   (today - timedelta(days=1)).isoformat(),
                        "timezone":   "Europe/Copenhagen",
                    },
                    timeout=20
                )
                r.raise_for_status()
                data = r.json()
                for date_str, precip in zip(data["daily"]["time"], data["daily"]["precipitation_sum"]):
                    if precip is None:
                        continue
                    rows.append({
                        "country":          country,
                        "date":             date_str,
                        "precipitation_mm": round(precip, 2),
                        "data_type":        "historisk",
                    })
                break
            except Exception as e:
                print(f"  Fejl historisk nedbør {country}: {e}")
                time.sleep(10)

        # Forecast
        for attempt in range(3):
            try:
                r = requests.get(
                    "https://api.open-meteo.com/v1/forecast",
                    params={
                        "latitude":   coords["lat"],
                        "longitude":  coords["lon"],
                        "daily":      "precipitation_sum",
                        "start_date": today.isoformat(),
                        "end_date":   date_to,
                        "timezone":   "Europe/Copenhagen",
                    },
                    timeout=20
                )
                r.raise_for_status()
                data = r.json()
                today_str = today.isoformat()
                for date_str, precip in zip(data["daily"]["time"], data["daily"]["precipitation_sum"]):
                    if precip is None:
                        continue
                    data_type = "i dag" if date_str == today_str else "forecast"
                    rows.append({
                        "country":          country,
                        "date":             date_str,
                        "precipitation_mm": round(precip, 2),
                        "data_type":        data_type,
                    })
                break
            except Exception as e:
                print(f"  Fejl forecast nedbør {country}: {e}")
                time.sleep(10)

        time.sleep(1)

    if rows:
        upsert_with_retry("hydro_weather_forecast", rows, "country,date", batch_size=200)
        print(f"  Nedbørsdata gemt ({len(rows)} rækker)")

if __name__ == "__main__":
    print(f"\n{'='*40}\nStart: {datetime.now()}\n{'='*40}")
    collect_realtid_dk_hourly()
    collect_realtime_prices()
    collect_temperature_realtime()
    collect_hydro_forecast_realtime()
    print(f"\nFærdig: {datetime.now()}\n{'='*40}")
