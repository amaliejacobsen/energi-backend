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


def collect_realtid_dk_hourly():
    print("Henter realtid data...")

    from_dt = (datetime.utcnow() - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M")
    rows_per_area = {"DK1": {}, "DK2": {}}

    for area in ["DK1", "DK2"]:
        offset = 0
        while True:
            r = None
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

            if r is None:
                break

            records = r.json().get("records", [])
            if not records:
                break

            for rec in records:
                dt_str = rec["HourDK"].replace("Z", "")
                dt_dk = datetime.fromisoformat(dt_str)
                dt_iso = dt_dk.isoformat()
                rows_per_area[area][dt_iso] = {
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


DK_NEIGHBORS = {
    "DK1": {
        "eic": "10YDK-1--------W",
        "neighbors": {
            "NO2": "10YNO-2--------T",
            "DE":  "10Y1001A1001A82H",
            "DK2": "10YDK-2--------M",
        }
    },
    "DK2": {
        "eic": "10YDK-2--------M",
        "neighbors": {
            "SE4": "10Y1001A1001A47J",
            "DE":  "10Y1001A1001A63L",
            "DK1": "10YDK-1--------W",
        }
    }
}


def fetch_physical_flows(in_eic, out_eic, date_str, token):
    params = {
        "documentType": "A11",
        "in_Domain":    in_eic,
        "out_Domain":   out_eic,
        "periodStart":  f"{date_str.replace('-', '')}0000",
        "periodEnd":    f"{date_str.replace('-', '')}2300",
        "securityToken": token,
    }
    for attempt in range(3):
        r = requests.get(ENTSOE_URL, params=params, timeout=60)
        if r.status_code == 200:
            break
        elif r.status_code in (503, 429):
            time.sleep(10 * (attempt + 1))
        else:
            return 0
    else:
        return 0

    try:
        root = ET.fromstring(r.text)
    except ET.ParseError:
        return 0

    ns = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0"}
    total = 0
    count = 0
    for ts in root.findall(".//ns:TimeSeries", ns):
        for period in ts.findall("ns:Period", ns):
            res_el = period.find("ns:resolution", ns)
            resolution = res_el.text if res_el is not None else "PT60M"
            for point in period.findall("ns:Point", ns):
                qty_el = point.find("ns:quantity", ns)
                if qty_el is None:
                    continue
                try:
                    qty = float(qty_el.text)
                    if resolution == "PT15M":
                        qty /= 4
                    total += qty
                    count += 1
                except ValueError:
                    continue
    return total / count if count > 0 else 0


def fetch_generation_mix(eic, date_str, token):
    params = {
        "documentType": "A75",
        "processType":  "A16",
        "in_Domain":    eic,
        "periodStart":  f"{date_str.replace('-', '')}0000",
        "periodEnd":    f"{date_str.replace('-', '')}2300",
        "securityToken": token,
    }
    for attempt in range(3):
        r = requests.get(ENTSOE_URL, params=params, timeout=60)
        if r.status_code == 200:
            break
        elif r.status_code in (503, 429):
            time.sleep(10 * (attempt + 1))
        else:
            return {}
    else:
        return {}

    try:
        root = ET.fromstring(r.text)
    except ET.ParseError:
        return {}

    ns = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
    result = defaultdict(float)
    counts = defaultdict(int)

    for ts in root.findall(".//ns:TimeSeries", ns):
        psr_el = ts.find(".//ns:psrType", ns)
        if psr_el is None:
            continue
        psr = PSR_NAMES.get(psr_el.text, psr_el.text)
        for period in ts.findall("ns:Period", ns):
            res_el = period.find("ns:resolution", ns)
            resolution = res_el.text if res_el is not None else "PT60M"
            for point in period.findall("ns:Point", ns):
                qty_el = point.find("ns:quantity", ns)
                if qty_el is None:
                    continue
                try:
                    qty = float(qty_el.text)
                    if resolution == "PT15M":
                        qty /= 4
                    result[psr] += qty
                    counts[psr] += 1
                except ValueError:
                    continue

    return {psr: result[psr] / counts[psr] for psr in result if counts[psr] > 0}

def fetch_all_records(dataset, area, start="2020-01-01", end=None):
    all_records = []
    limit = 10000
    offset = 0
    sort_column = "TimeDK" if dataset == "DayAheadPrices" else "HourDK"
    rate_limit_attempts = 0
    while True:
        try:
            # RETTELSE 1: Vi bygger params sikkert så den ikke crasher hvis global 'end' mangler
            params = {
                "start": start,
                "filter": f'{{"PriceArea":"{area}"}}',
                "limit": limit,
                "offset": offset,
                "sort": f"{sort_column} asc",
            }
            if end is not None:
                params["end"] = end

            r = requests.get(f"https://api.energidataservice.dk/dataset/{dataset}", params=params, timeout=30)
            
            if r.status_code == 429:
                rate_limit_attempts += 1   
                ventetid = 60 * rate_limit_attempts  
                # RETTELSE 2: Dynamisk ventetid vises nu korrekt i printet
                print(f"  Rate limit ramt for {dataset} ({area}) ved offset {offset}, venter {ventetid}s...")
                time.sleep(ventetid)
                continue
            rate_limit_attempts = 0 
            
            # RETTELSE 3: Fjernet den ekstra dublerede r.raise_for_status()
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
            time.sleep(2)  
        except Exception as e:
            print(f"Fejl ved hentning af {dataset} ({area}): {e}")
            break
    return all_records


def fetch_dk_production_today(area):
    """Henter dagens sol og vind i REEL REALTID fra ElectricityBalanceNonv."""
    today = current_date.strftime("%Y-%m-%dT00:00")
    
    # Debug print: Se præcis hvilken dato koden spørger efter data fra
    print(f"  -> [DEBUG] Henter sol/vind for {area} med start: {today}")
    
    records = fetch_all_records("ElectricityBalanceNonv", area, start=today)
    
    # Debug print: Se hvor mange rækker API'et rent faktisk returnerede
    print(f"  -> [DEBUG] Modtog {len(records)} rækker fra API'et for {area}")
    
    solar_total, offshore_total, onshore_total, count = 0, 0, 0, 0
    
    for rec in records:
        solar_total += rec.get("SolarPower", 0) or 0
        offshore_total += rec.get("OffshoreWindPower", 0) or 0
        onshore_total += rec.get("OnshoreWindPower", 0) or 0
        count += 1
        
    if count == 0:
        print(f"  -> [DEBUG] Ingen rækker at beregne for {area}, returnerer tomt.")
        return {}
        
    res = {
        "Solar": round(solar_total / count, 2),
        "Wind Offshore": round(offshore_total / count, 2),
        "Wind Onshore": round(onshore_total / count, 2),
    }
    
    # Debug print: Se hvad resultatet af beregningen blev
    print(f"  -> [DEBUG] Beregnet snit for {area}: {res}")
    return res

def collect_generation_mix():
    print("Henter generation mix...")
    date_str = current_date.strftime("%Y-%m-%d")
    rows = []
    for area, config in DK_NEIGHBORS.items():
        eic = config["eic"]
        gen_mix = fetch_generation_mix(eic, date_str, ENTSOE_TOKEN)
        print(f"  {area} gen_mix: {gen_mix}")
        for psr_name, avg_mw in gen_mix.items():
            rows.append({
                "area":      area,
                "date":      date_str,
                "source":    psr_name,
                "avg_mw":    round(avg_mw, 2),
                "is_import": False,
            })
        # Tilføj sol og vind fra Energidataservice
        dk_prod = fetch_dk_production_today(area)
        for source, avg_mw in dk_prod.items():
            if avg_mw > 0:
                rows.append({
                    "area":      area,
                    "date":      date_str,
                    "source":    source,
                    "avg_mw":    avg_mw,
                    "is_import": False,
                })
        for neighbor_name, neighbor_eic in config["neighbors"].items():
            imp = fetch_physical_flows(neighbor_eic, eic, date_str, ENTSOE_TOKEN)
            exp = fetch_physical_flows(eic, neighbor_eic, date_str, ENTSOE_TOKEN)
            net_import = imp - exp
            print(f"  {area} ← {neighbor_name}: imp={imp:.0f} exp={exp:.0f} net={net_import:.0f}")
            if net_import > 0:
                rows.append({
                    "area":      area,
                    "date":      date_str,
                    "source":    f"Fra {neighbor_name}",
                    "avg_mw":    round(net_import, 2),
                    "is_import": True,
                })
            time.sleep(1)
    if rows:
        # RETTET: Det mærkelige tegn er fjernet fra f-stringen herunder
        print("\n--- GENERATION MIX DATA DER SENDES TIL SUPABASE ---")
        for r in rows:
            print(f"Area: {r['area']} | Source: {r['source']:<22} | MW: {r['avg_mw']:<8} | Import: {r['is_import']}")
        print("---------------------------------------------------\n")

        supabase.table("generation_mix").upsert(
            rows, on_conflict="area,date,source"
        ).execute()
        print(f"Generation mix gemt ({len(rows)} rækker).")


if __name__ == "__main__":
    print(f"\n{'='*40}\nStart: {datetime.now()}\n{'='*40}")
    collect_realtid_dk_hourly()
    collect_generation_mix()
    print(f"\nFærdig: {datetime.now()}\n{'='*40}")
