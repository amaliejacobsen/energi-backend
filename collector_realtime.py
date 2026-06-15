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
            for attempt in range(5):  # ← retry loop
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
                    rec = r.json()["records"][0]
                    print("Solar:", rec.get("SolarPower"))
                    print("Offshore:", rec.get("OffshoreWindPower"))
                    print("Onshore:", rec.get("OnshoreWindPower"))
                    print("Consumption:", rec.get("GrossConsumption"))
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
                dt_dk = datetime.fromisoformat(dt_str)
                dt_iso = dt_dk.isoformat()  # ← behold dansk tid med +00:00 format
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


DK_NEIGHBORS = {
    "DK1": {
        "eic": "10YDK-1--------W",
        "neighbors": {
            "NO2":     "10YNO-2--------T",
            "DE":      "10Y1001A1001A82H",
            "DK2":     "10YDK-2--------M",
        }
    },
    "DK2": {
        "eic": "10YDK-2--------M",
        "neighbors": {
            "SE4":     "10Y1001A1001A47J",
            "DE":      "10Y1001A1001A63L",
            "DK1":     "10YDK-1--------W",
        }
    }
}

def fetch_physical_flows(in_eic, out_eic, date_str, token):
    """Henter fysiske flows fra in_Domain til out_Domain for en given dato."""
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
    """Henter lokal produktionsmix (A75) for et prisområde."""
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

def collect_generation_mix():
    print("Henter generation mix...")
    date_str = current_date.strftime("%Y-%m-%d")
    rows = []

    for area, config in DK_NEIGHBORS.items():
        eic = config["eic"]

        # Lokal produktion
        gen_mix = fetch_generation_mix(eic, date_str, ENTSOE_TOKEN)
        for psr_name, avg_mw in gen_mix.items():
            rows.append({
                "area":     area,
                "date":     date_str,
                "source":   psr_name,
                "avg_mw":   round(avg_mw, 2),
                "is_import": False,
            })

        # Import fra nabolande
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
        supabase.table("generation_mix").upsert(
            rows, on_conflict="area,date,source"
        ).execute()
        print(f"Generation mix gemt ({len(rows)} rækker).")
        
if __name__ == "__main__":
    print(f"\n{'='*40}\nStart: {datetime.now()}\n{'='*40}")
    collect_realtid_dk_hourly()
    collect_generation_mix()
    print(f"\nFærdig: {datetime.now()}\n{'='*40}")
