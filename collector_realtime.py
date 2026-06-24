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

end = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")

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

def fetch_all_records(dataset, area, start="2020-01-01"):
    all_records = []
    limit = 10000
    offset = 0
    sort_column = "TimeDK" if dataset == "DayAheadPrices" else "HourDK"
    while True:
        try:
            r = requests.get(f"https://api.energidataservice.dk/dataset/{dataset}", params={
                "start": start,
                "end": end,
                "filter": f'{{"PriceArea":"{area}"}}',
                "limit": limit,
                "offset": offset,
                "sort": f"{sort_column} asc",
            }, timeout=30)
            
            if r.status_code == 429:
                print(f"  Rate limit ramt for {dataset} ({area}) ved offset {offset}, venter 30s...")
                time.sleep(30)
                continue
                
            r.raise_for_status()
            if not r.text.strip():
                break
            data = r.json()
            records = data.get("records", [])
            if not records:
                break
            all_records.extend(records)
            if len(records) < limit:
                break
            offset += limit
            time.sleep(2)  # Øget fra 0.3 til 2 sekunder
        except Exception as e:
            print(f"Fejl ved hentning af {dataset} ({area}): {e}")
            break
    return all_records


def collect_dk_hourly_data():
    print("Henter DK timesdata...")
    
    for area in ["DK1", "DK2"]: 
        price_dict = {}
        
        # Kun hent de seneste 7 dage + i morgen (day-ahead)
        recent_start = (current_date - timedelta(days=7)).strftime("%Y-%m-%d")
        
        for rec in fetch_all_records("Elspotprices", area, start=recent_start):
            dt_dk = datetime.fromisoformat(rec["HourDK"].replace('Z', ''))
            dt_dk = dt_dk + timedelta(hours=2)  # ← tilføj denne
            dt_str = dt_dk.strftime("%Y-%m-%dT%H:%M:%S")
            price_dict[dt_str] = {
                "area": area,
                "datetime": dt_str,
                "price_dkk": rec["SpotPriceDKK"]
            }
        
        for rec in fetch_all_records("DayAheadPrices", area, start=recent_start):
            dt_dk = datetime.fromisoformat(rec["TimeDK"].replace('Z', ''))
            dt_dk = dt_dk + timedelta(hours=2)  # ← tilføj denne
            dt_str = dt_dk.strftime("%Y-%m-%dT%H:%M:%S")
            if dt_str not in price_dict:
                price_dict[dt_str] = {
                    "area": area,
                    "datetime": dt_str,
                    "price_dkk": rec["DayAheadPriceDKK"]
                }
        
        price_rows = list(price_dict.values())
        if price_rows:
            for i in range(0, len(price_rows), 1000):
                supabase.table("dk_prices_hourly").upsert(
                    price_rows[i:i+1000], 
                    on_conflict="area,datetime"
                ).execute()
            print(f"  {area} priser gemt ({len(price_rows)} rækker)")

        # Produktion — behold start=2020 kun første gang, herefter kun recent
        # ... resten af funktionen uændret

        # Produktion — historisk fra ProductionConsumptionSettlement
        solar_dict = {}
        offshore_dict = {}
        onshore_dict = {}
        consumption_dict = {}

        for rec in fetch_all_records("ProductionConsumptionSettlement", area):
            dt = datetime.fromisoformat(rec["HourDK"].replace('Z', '+00:00'))
            dt_iso = rec["HourDK"].replace('Z', '')
            solar    = (rec.get("SolarPowerLt10kW_MWh", 0) or 0) + \
                       (rec.get("SolarPowerGe10Lt40kW_MWh", 0) or 0) + \
                       (rec.get("SolarPowerGe40kW_MWh", 0) or 0)
            offshore = (rec.get("OffshoreWindLt100MW_MWh", 0) or 0) + \
                       (rec.get("OffshoreWindGe100MW_MWh", 0) or 0)
            onshore  = (rec.get("OnshoreWindLt50kW_MWh", 0) or 0) + \
                       (rec.get("OnshoreWindGe50kW_MWh", 0) or 0)
            consumption = rec.get("GrossConsumption_MWh", 0) or 0
            solar_dict[dt_iso]       = {"area": area, "source": "solar",       "datetime": dt_iso, "value_mwh": solar}
            offshore_dict[dt_iso]    = {"area": area, "source": "offshore",    "datetime": dt_iso, "value_mwh": offshore}
            onshore_dict[dt_iso]     = {"area": area, "source": "onshore",     "datetime": dt_iso, "value_mwh": onshore}
            consumption_dict[dt_iso] = {"area": area, "source": "consumption", "datetime": dt_iso, "value_mwh": consumption}


        for source, d in [("solar", solar_dict), ("offshore", offshore_dict), ("onshore", onshore_dict), ("consumption", consumption_dict)]:
            rows = list(d.values())
            if rows:
                for i in range(0, len(rows), 1000):
                    supabase.table("dk_production_hourly").upsert(
                        rows[i:i+1000], on_conflict="area,source,datetime"
                    ).execute()
                print(f"  {area} {source} gemt ({len(rows)} rækker)")


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
                dt_iso = dt_str
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
    print("Henter generation mix (GenerationProdTypeExchange) - akkumuleret fra kl. 00...")
    date_str = datetime.utcnow().strftime("%Y-%m-%d")

    for area in ["DK1", "DK2"]:
        if area == "DK2":
            print("  Pause før DK2...")
            time.sleep(30)

        all_records = []
        offset = 0
        while True:
            r = None
            for attempt in range(5):
                try:
                    r = requests.get(
                        "https://api.energidataservice.dk/dataset/GenerationProdTypeExchange",
                        params={
                            "filter": f'{{"PriceArea":"{area}"}}',
                            "start": f"{date_str}T00:00",
                            "limit": 1000,
                            "offset": offset,
                            "sort": "TimeDK asc",
                        },
                        timeout=30
                    )
                    if r.status_code == 429:
                        wait = 30 * (attempt + 1)
                        print(f"  Rate limit for {area}, venter {wait}s...")
                        time.sleep(wait)
                        continue
                    r.raise_for_status()
                    break
                except Exception as e:
                    print(f"  Fejl for {area}: {e}, venter 15s...")
                    time.sleep(15)
            else:
                print(f"  Alle forsøg fejlede for {area}, springer over.")
                break

            if r is None:
                break

            records = r.json().get("records", [])
            if not records:
                break
            all_records.extend(records)
            if len(records) < 1000:
                break
            offset += 1000
            time.sleep(1)

        if not all_records:
            print(f"  Ingen data for {area}")
            continue

        print(f"  {area}: {len(all_records)} målinger fra {all_records[0].get('TimeDK')} til {all_records[-1].get('TimeDK')}")

        source_fields = {
            "Offshore Wind":  "OffshoreWindPower",
            "Onshore Wind":   "OnshoreWindPower",
            "Solar":          "SolarPower",
            "Hydro":          "HydroPower",
            "Biomass":        "Biomass",
            "Biogas":         "Biogas",
            "Waste":          "Waste",
            "Fossil Gas":     "FossilGas",
            "Fossil Oil":     "FossilOil",
            "Fossil Hard coal": "FossilHardCoal",
        }
        exchange_fields = {
            "Fra Tyskland":      "ExchangeGermany",
            "Fra Sverige":       "ExchangeSweden",
            "Fra Norge":         "ExchangeNorway",
            "Fra Holland":       "ExchangeNetherlands",
            "Fra Storbritannien": "ExchangeGreatBritain",
            "Fra DK1/DK2":       "ExchangeGreatBelt",
        }

        def avg_field(field):
            vals = [rec.get(field, 0) or 0 for rec in all_records]
            return sum(vals) / len(vals) if vals else 0

        rows = []
        for source, field in source_fields.items():
            rows.append({
                "area": area, "date": date_str, "source": source,
                "avg_mw": round(avg_field(field), 2), "is_import": False,
            })
        for source, field in exchange_fields.items():
            avg_mw = avg_field(field)
            if avg_mw == 0:
                continue
            rows.append({
                "area": area, "date": date_str, "source": source,
                "avg_mw": round(avg_mw, 2), "is_import": avg_mw > 0,
            })

        print(f"\n--- {area} KLAR TIL AT GEMME ---")
        for row in rows:
            print(f"{row['source']:<22} | {row['avg_mw']} MW | import={row['is_import']}")
        print("-------------------------\n")

        supabase.table("generation_mix").upsert(rows, on_conflict="area,date,source").execute()
        print(f"{area} generation mix gemt ({len(rows)} rækker).")


if __name__ == "__main__":
    print(f"\n{'='*40}\nStart: {datetime.now()}\n{'='*40}")
    collect_realtid_dk_hourly()
    collect_generation_mix()
    collect_dk_hourly_data()
    print(f"\nFærdig: {datetime.now()}\n{'='*40}")
