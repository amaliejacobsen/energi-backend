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
    print("Henter realtid data...")

    from_dt = (datetime.utcnow() - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M")
    rows_per_area = {"DK1": {}, "DK2": {}}

    for area in ["DK1", "DK2"]:
        if area == "DK2":
            print("  Pause før DK2...")
            time.sleep(60)
        offset = 0
        while True:
            r = None
            for attempt in range(10):
                try:
                    r = requests.get(
                        "https://api.energidataservice.dk/dataset/ElectricityProdex5MinRealtime",
                        params={
                            "filter": f'{{"PriceArea":"{area}"}}',
                            "start": from_dt,
                            "limit": 1000,
                            "offset": offset,
                            "sort": "Minutes5UTC asc",
                        },
                        timeout=30
                    )
                    if r.status_code == 429:
                        wait = 60 * (attempt + 1)
                        print(f"  Rate limit, venter {wait}s...")
                        time.sleep(wait)
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
                dt_str = rec["Minutes5DK"].replace("Z", "")
                dt_dk = datetime.fromisoformat(dt_str)
                dt_utc = dt_dk - timedelta(hours=2)
                dt_str_utc = dt_utc.strftime("%Y-%m-%dT%H:%M:%S")

                exchange_abroad = (
                    (rec.get("ExchangeGermany", 0) or 0) +
                    (rec.get("ExchangeNetherlands", 0) or 0) +
                    (rec.get("ExchangeGreatBritain", 0) or 0) +
                    (rec.get("ExchangeNorway", 0) or 0) +
                    (rec.get("ExchangeSweden", 0) or 0) +
                    (rec.get("BornholmSE4", 0) or 0)
                )
                production = (
                    (rec.get("ProductionLt100MW", 0) or 0) +
                    (rec.get("ProductionGe100MW", 0) or 0) +
                    (rec.get("OffshoreWindPower", 0) or 0) +
                    (rec.get("OnshoreWindPower", 0) or 0) +
                    (rec.get("SolarPower", 0) or 0)
                )
                rows_per_area[area][dt_str_utc] = {
                    "solar":       rec.get("SolarPower", 0) or 0,
                    "offshore":    rec.get("OffshoreWindPower", 0) or 0,
                    "onshore":     rec.get("OnshoreWindPower", 0) or 0,
                    "consumption": (production - exchange_abroad) * (5/60),
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


def fetch_physical_flows(in_eic, out_eic, start_str, end_str, token):
    params = {
        "documentType": "A11",
        "in_Domain":    in_eic,
        "out_Domain":   out_eic,
        "periodStart":  start_str,
        "periodEnd":    end_str,
        "securityToken": token,
    }
    for attempt in range(3):
        try:
            r = requests.get(ENTSOE_URL, params=params, timeout=90)  # ← op fra 60
            if r.status_code == 200:
                break
            elif r.status_code in (503, 429):
                time.sleep(10 * (attempt + 1))
            else:
                return 0
        except requests.exceptions.ReadTimeout:
            print(f"  Timeout ved physical flows (forsøg {attempt+1}/3), venter 30s...")
            time.sleep(30)
        except Exception as e:
            print(f"  Fejl ved physical flows: {e}")
            return 0
    else:
        print(f"  Alle forsøg fejlede for {in_eic} → {out_eic}, returnerer 0")
        return 0
    # ... resten uændret
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


def fetch_generation_mix(eic, start_str, end_str, token):
    params = {
        "documentType": "A75",
        "processType":  "A16",
        "in_Domain":    eic,
        "periodStart":  start_str,
        "periodEnd":    end_str,
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


def fetch_generation_mix_realtime(eic, token):
    """Henter seneste tilgængelige A75 generation data (typisk 1-3 timer forsinket)."""
    now = datetime.utcnow()
    period_start = (now - timedelta(hours=6)).strftime("%Y%m%d%H00")
    period_end   = now.strftime("%Y%m%d%H00")

    params = {
        "documentType": "A75",
        "processType":  "A16",
        "in_Domain":    eic,
        "periodStart":  period_start,
        "periodEnd":    period_end,
        "securityToken": token,
    }
    r = requests.get(ENTSOE_URL, params=params, timeout=60)
    print(f"  Status: {r.status_code}")
    if r.status_code != 200:
        print(f"  Fejl-tekst: {r.text[:300]}")
        return {}

    try:
        root = ET.fromstring(r.text)
    except ET.ParseError:
        print("  Kunne ikke parse XML")
        return {}

    ns = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
    latest_values = {}

    for ts in root.findall(".//ns:TimeSeries", ns):
        psr_el = ts.find(".//ns:psrType", ns)
        if psr_el is None:
            continue
        psr_code = psr_el.text
        psr = PSR_NAMES.get(psr_code, psr_code)

        for period in ts.findall("ns:Period", ns):
            start_el = period.find("ns:timeInterval/ns:start", ns)
            res_el   = period.find("ns:resolution", ns)
            if start_el is None or res_el is None:
                continue

            start_dt = datetime.strptime(start_el.text, "%Y-%m-%dT%H:%MZ")
            resolution = res_el.text
            step_min = 15 if resolution == "PT15M" else 60

            for point in period.findall("ns:Point", ns):
                pos_el = point.find("ns:position", ns)
                qty_el = point.find("ns:quantity", ns)
                if pos_el is None or qty_el is None:
                    continue
                try:
                    pos = int(pos_el.text)
                    qty = float(qty_el.text)
                    point_time = start_dt + timedelta(minutes=(pos - 1) * step_min)
                    if psr not in latest_values or point_time > latest_values[psr]["time"]:
                        latest_values[psr] = {"time": point_time, "value": qty}
                except ValueError:
                    continue

    # DEBUG
    for psr, v in latest_values.items():
        print(f"    {psr}: {v['value']} MW (tid: {v['time']})")

    return {psr: v["value"] for psr, v in latest_values.items()}


def collect_generation_mix():
    print("Henter generation mix fra ENTSO-E (realtid)...")
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    rows = []

    eics = {"DK1": "10YDK-1--------W", "DK2": "10YDK-2--------M"}

    for area, eic in eics.items():
        print(f"  {area}...")
        gen_mix = fetch_generation_mix_realtime(eic, ENTSOE_TOKEN)
        for source, mw in gen_mix.items():
            rows.append({
                "area":      area,
                "date":      date_str,
                "source":    source,
                "avg_mw":    round(mw, 2),
                "is_import": False,
            })

    if rows:
        print("\n--- KLAR TIL AT GEMME ---")
        for row in rows:
            print(f"{row['area']} | {row['source']:<25} | {row['avg_mw']} MW")
        print("-------------------------\n")
        # supabase.table("generation_mix").upsert(rows, on_conflict="area,date,source").execute()
        print("(Gemning er udkommenteret for nu - tjek værdierne først)")
    else:
        print("  Ingen rækker.")


if __name__ == "__main__":
    print(f"\n{'='*40}\nStart: {datetime.now()}\n{'='*40}")
    collect_realtid_dk_hourly()
    collect_generation_mix()
    print(f"\nFærdig: {datetime.now()}\n{'='*40}")
