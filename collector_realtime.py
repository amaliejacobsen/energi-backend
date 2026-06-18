import requests
import xml.etree.ElementTree as ET
import time
from datetime import datetime, timedelta
from collections import defaultdict
from supabase import create_client
import os

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase     = create_client(SUPABASE_URL, SUPABASE_KEY)

ENTSOE_TOKEN = os.environ.get("ENTSOE_TOKEN", "138899c3-59b3-48ef-9dfd-03406794210d")
ENTSOE_URL   = "https://web-api.tp.entsoe.eu/api"

current_date = datetime.today()

PSR_NAMES = {
    "B01": "Biomass", "B02": "Fossil Brown coal/Lignite", "B03": "Fossil Coal-derived gas",
    "B04": "Fossil Gas", "B05": "Fossil Hard coal", "B06": "Fossil Oil",
    "B09": "Hydro Pumped Storage", "B10": "Hydro Run-of-river",
    "B11": "Hydro Water Reservoir", "B12": "Wind Offshore", "B13": "Wind Onshore",
    "B14": "Solar", "B16": "Nuclear", "B17": "Other renewable",
    "B18": "Waste", "B19": "Other", "B20": "Marine",
}

DK_NEIGHBORS = {
    "DK1": {
        "eic": "10YDK-1--------W",
        "neighbors": {
            "NO2": "10YNO-2--------T",
            "DE":  "10Y1001A1001A83F",  # ← ret denne
            "NL":  "10YNL----------L",  # ← tilføj Holland
            "DK2": "10YDK-2--------M",
            "SE3": "10Y1001A1001A46L",  # ← tilføj Sverige SE3
        }
    },
    "DK2": {
        "eic": "10YDK-2--------M",
        "neighbors": {
            "SE4": "10Y1001A1001A47J",
            "DE":  "10Y1001A1001A83F",  # ← ret denne
            "DK1": "10YDK-1--------W",
        }
    }
}

def collect_realtid_dk_hourly():
    print("Henter realtid data (GenerationProdTypeExchange)...")

    from_dt = (datetime.utcnow() - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M")
    rows_per_area = {"DK1": {}, "DK2": {}}

    for area in ["DK1", "DK2"]:
        offset = 0
        while True:
            r = None
            for attempt in range(5):
                try:
                    r = requests.get(
                        "https://api.energidataservice.dk/dataset/GenerationProdTypeExchange",
                        params={
                            "filter": f'{{"PriceArea":"{area}"}}',
                            "start": from_dt,
                            "limit": 1000,
                            "offset": offset,
                            "sort": "TimeDK asc",
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

            if r is None:
                break

            records = r.json().get("records", [])
            if not records:
                break

            # DEBUG - vis felter første gang
            if offset == 0:
                print(f"  FELTER {area}:", list(records[0].keys()))

            for rec in records:
                dt_str = rec.get("TimeDK", "").replace("Z", "")
                if not dt_str:
                    continue
                dt_iso = datetime.fromisoformat(dt_str).isoformat()
                rows_per_area[area][dt_iso] = {
                    "solar":       rec.get("SolarPower", 0) or 0,
                    "offshore":    rec.get("OffshoreWindPower", 0) or 0,
                    "onshore":     rec.get("OnshoreWindPower", 0) or 0,
                    "consumption": rec.get("TotalLoad", 0) or 0,
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
                    "area":      area,
                    "source":    source,
                    "datetime":  dt_str,
                    "value_mwh": round(vals[source], 3),
                })

        if rows:
            for i in range(0, len(rows), 1000):
                supabase.table("dk_production_hourly").upsert(
                    rows[i:i+1000],
                    on_conflict="area,source,datetime"
                ).execute()
            print(f"  {area} realtid gemt ({len(rows)} rækker)")



def collect_generation_mix():
    print("Henter generation mix (GenerationProdTypeExchange)...")
    date_str = datetime.utcnow().strftime("%Y-%m-%d")

    for area in ["DK1", "DK2"]:
        r = requests.get(
            "https://api.energidataservice.dk/dataset/GenerationProdTypeExchange",
            params={
                "filter": f'{{"PriceArea":"{area}"}}',
                "start": (datetime.utcnow() - timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M"),
                "limit": 100,
                "sort": "TimeDK desc",
            },
            timeout=30
        )
        if r.status_code != 200:
            print(f"  Fejl for {area}: {r.status_code}")
            continue

        records = r.json().get("records", [])
        if not records:
            print(f"  Ingen data for {area}")
            continue

        rec = records[0]  # seneste tidspunkt
        print(f"  {area} tidspunkt: {rec.get('TimeDK')}")

        sources = {
            "Offshore Wind":  rec.get("OffshoreWindPower", 0) or 0,
            "Onshore Wind":   rec.get("OnshoreWindPower", 0) or 0,
            "Solar":          rec.get("SolarPower", 0) or 0,
            "Hydro":          rec.get("HydroPower", 0) or 0,
            "Biomass":        rec.get("Biomass", 0) or 0,
            "Biogas":         rec.get("Biogas", 0) or 0,
            "Waste":          rec.get("Waste", 0) or 0,
            "Fossil Gas":     rec.get("FossilGas", 0) or 0,
            "Fossil Oil":     rec.get("FossilOil", 0) or 0,
            "Fossil Hard coal": rec.get("FossilHardCoal", 0) or 0,
        }

        exchanges = {
            "Fra Tyskland":      rec.get("ExchangeGermany", 0) or 0,
            "Fra Sverige":       rec.get("ExchangeSweden", 0) or 0,
            "Fra Norge":         rec.get("ExchangeNorway", 0) or 0,
            "Fra Holland":       rec.get("ExchangeNetherlands", 0) or 0,
            "Fra Storbritannien": rec.get("ExchangeGreatBritain", 0) or 0,
            "Fra DK1/DK2":       rec.get("ExchangeGreatBelt", 0) or 0,
        }

        rows = []
        for source, mw in sources.items():
            rows.append({
                "area":      area,
                "date":      date_str,
                "source":    source,
                "avg_mw":    round(mw, 2),
                "is_import": False,
            })
        for source, mw in exchanges.items():
            if mw > 0:  # kun positiv = faktisk import
                rows.append({
                    "area":      area,
                    "date":      date_str,
                    "source":    source,
                    "avg_mw":    round(mw, 2),
                    "is_import": True,
                })

        print(f"\n--- {area} KLAR TIL AT GEMME ---")
        for row in rows:
            print(f"{row['source']:<22} | {row['avg_mw']} MW | import={row['is_import']}")
        print("-------------------------\n")

        # supabase.table("generation_mix").upsert(rows, on_conflict="area,date,source").execute()
        print("(Gemning er udkommenteret for nu - tjek værdierne først)")

if __name__ == "__main__":
    print(f"\n{'='*40}\nStart: {datetime.now()}\n{'='*40}")
    collect_realtid_dk_hourly()
    collect_generation_mix()
    print(f"\nFærdig: {datetime.now()}\n{'='*40}")
