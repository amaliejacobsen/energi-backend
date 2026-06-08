import requests
import time
from datetime import datetime, timedelta
from supabase import create_client
import os

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase     = create_client(SUPABASE_URL, SUPABASE_KEY)

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
            
            # Print felter første gang for debug
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
            for i in range(0, len(rows), 1000):
                supabase.table("dk_production_hourly").upsert(
                    rows[i:i+1000],
                    on_conflict="area,source,datetime"
                ).execute()
            print(f"  {area} realtid gemt ({len(rows) // 4} tidspunkter)")

if __name__ == "__main__":
    print(f"\n{'='*40}\nStart: {datetime.now()}\n{'='*40}")
    collect_realtid_dk_hourly()
    print(f"\nFærdig: {datetime.now()}\n{'='*40}")
