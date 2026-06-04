import requests
import time
from datetime import datetime, timedelta
from supabase import create_client
import os

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase     = create_client(SUPABASE_URL, SUPABASE_KEY)

def collect_realtid_dk_hourly():
    print("Henter realtid data...")
    
    from_dt = (datetime.utcnow() - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M")
    rows_per_area = {"DK1": {}, "DK2": {}}
    
    for area in ["DK1", "DK2"]:
        offset = 0
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
                        timeout=30
                    )
                    if r.status_code == 429:
                        print(f"  Rate limit, venter 30s...")
                        time.sleep(30)
                        continue
                    r.raise_for_status()
                    break
                except Exception as e:
                    print(f"  Fejl: {e}, venter 15s...")
                    time.sleep(15)
            
            records = r.json().get("records", [])
            if not records:
                break
            
            for rec in records:
                dt_str = rec["HourDK"].replace("Z", "")
                rows_per_area[area][dt_str] = {
                    "solar":       rec.get("SolarPower", 0) or 0,
                    "offshore":    rec.get("OffshoreWindPower", 0) or 0,
                    "onshore":     rec.get("OnshoreWindPower", 0) or 0,
                    "consumption": rec.get("GrossConsumption", 0) or 0,
                }
            
            if len(records) < 1000:
                break
            offset += 1000
            time.sleep(2)
    
    for area, timepoints in rows_per_area.items():
        rows = []
        for dt_str, vals in timepoints.items():
            for source in ["solar", "offshore", "onshore", "consumption"]:
                rows.append({
                    "area": area,
                    "source": source,
                    "datetime": dt_str,
                    "value_mwh": round(vals[source], 3),
                })
        
        if rows:
            for i in range(0, len(rows), 1000):
                supabase.table("dk_production_hourly").upsert(
                    rows[i:i+1000],
                    on_conflict="area,source,datetime"
                ).execute()
            print(f"  {area} realtid gemt ({len(rows)} rækker)")

if __name__ == "__main__":
    print(f"\n{'='*40}\nStart: {datetime.now()}\n{'='*40}")
    collect_realtid_dk_hourly()
    print(f"\nFærdig: {datetime.now()}\n{'='*40}")
