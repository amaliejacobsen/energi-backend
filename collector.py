import requests
import xml.etree.ElementTree as ET
import statistics
import time
from datetime import datetime, timedelta
from collections import defaultdict
from supabase import create_client
import os

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase     = create_client(SUPABASE_URL, SUPABASE_KEY)

ENTSOE_TOKEN    = os.environ.get("ENTSOE_TOKEN", "138899c3-59b3-48ef-9dfd-03406794210d")
ENTSOE_URL      = "https://web-api.tp.entsoe.eu/api"
AGSI_KEY        = os.environ.get("AGSI_KEY", "12a5ed2eb1b9d3f2091abd2213758ec2")
AGSI_URL        = "https://agsi.gie.eu/api"

current_date    = datetime.today()
end             = current_date.strftime("%Y-%m-%d")
current_year    = current_date.year
current_month   = current_date.month
current_day     = current_date.day

if current_day >= 14:
    if current_month > 1:
        last_full_month = current_month - 1
    else:
        last_full_month = 12
else:
    if current_month > 2:
        last_full_month = current_month - 2
    elif current_month == 2:
        last_full_month = 12
    else:
        last_full_month = 11

if last_full_month == 12 and current_month < 3:
    last_full_year = current_year - 1
else:
    last_full_year = current_year

print(f"Dato: {current_date.strftime('%Y-%m-%d')} | Henter data til og med: {last_full_year}-{last_full_month:02d}")

fetch_years     = list(range(2020, current_year + 1))
areas           = ["DK1", "DK2"]

def weighted_avg(price_dict, prod_dict):
    sumproduct = 0
    total_prod = 0
    for time_key, prod in prod_dict.items():
        if prod == 0:
            continue
        price = price_dict.get(time_key)
        if price is None:
            continue
        sumproduct += price * prod
        total_prod += prod
    return sumproduct / total_prod if total_prod > 0 else 0

def monthly_avg_prices(price_dict):
    monthly = defaultdict(list)
    for dt, price in price_dict.items():
        monthly[dt.strftime("%Y-%m")].append(price)
    return {m: sum(v) / len(v) for m, v in monthly.items()}

def monthly_weighted(price_dict, prod_dict):
    monthly_prices = defaultdict(dict)
    monthly_prod   = defaultdict(dict)
    for dt, price in price_dict.items():
        monthly_prices[dt.strftime("%Y-%m")][dt] = price
    for dt, prod in prod_dict.items():
        monthly_prod[dt.strftime("%Y-%m")][dt] = prod
    return {
        month: weighted_avg(monthly_prices.get(month, {}), monthly_prod[month])
        for month in monthly_prod
    }

def is_too_recent(year, month):
    if year > last_full_year:
        return True
    if year == last_full_year and month > last_full_month:
        return True
    return False

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
            time.sleep(0.3)
        except Exception as e:
            print(f"Fejl ved hentning af {dataset} ({area}): {e}")
            break
    return all_records


def collect_dk_data():
    print("Henter DK data...")
    hourly_prices = {area: {} for area in areas}
    solar_prod    = {area: {} for area in areas}
    offshore_prod = {area: {} for area in areas}
    onshore_prod  = {area: {} for area in areas}

    for area in areas:
        for rec in fetch_all_records("Elspotprices", area):
            dt = datetime.fromisoformat(rec["HourDK"].replace('Z', '+00:00'))
            if is_too_recent(dt.year, dt.month):
                continue
            hourly_prices[area][dt] = rec["SpotPriceDKK"]

        for rec in fetch_all_records("DayAheadPrices", area):
            dt = datetime.fromisoformat(rec["TimeDK"].replace('Z', '+00:00'))
            if is_too_recent(dt.year, dt.month):
                continue
            if dt not in hourly_prices[area]:
                hourly_prices[area][dt] = rec["DayAheadPriceDKK"]

        for rec in fetch_all_records("ProductionConsumptionSettlement", area):
            dt = datetime.fromisoformat(rec["HourDK"].replace('Z', '+00:00'))
            if is_too_recent(dt.year, dt.month):
                continue
            solar_prod[area][dt] = (rec.get("SolarPowerLt10kW_MWh", 0) or 0) + \
                                   (rec.get("SolarPowerGe10Lt40kW_MWh", 0) or 0) + \
                                   (rec.get("SolarPowerGe40kW_MWh", 0) or 0)
            offshore_prod[area][dt] = (rec.get("OffshoreWindLt100MW_MWh", 0) or 0) + \
                                      (rec.get("OffshoreWindGe100MW_MWh", 0) or 0)
            onshore_prod[area][dt] = (rec.get("OnshoreWindLt50kW_MWh", 0) or 0) + \
                                     (rec.get("OnshoreWindGe50kW_MWh", 0) or 0)

    for area in areas:
        avg_prices   = monthly_avg_prices(hourly_prices[area])
        avg_solar    = monthly_weighted(hourly_prices[area], solar_prod[area])
        avg_offshore = monthly_weighted(hourly_prices[area], offshore_prod[area])
        avg_onshore  = monthly_weighted(hourly_prices[area], onshore_prod[area])

        all_months = sorted(set(avg_prices) | set(avg_solar) | set(avg_offshore) | set(avg_onshore))
        rows = []
        for month_str in all_months:
            y, m = map(int, month_str.split("-"))
            if is_too_recent(y, m):
                continue
            spot  = avg_prices.get(month_str, 0)
            solar = avg_solar.get(month_str, 0)
            offsh = avg_offshore.get(month_str, 0)
            onsh  = avg_onshore.get(month_str, 0)
            rows.append({
                "area": area, "month": month_str,
                "spot_price": spot,
                "solar_weighted": solar,
                "offshore_weighted": offsh,
                "onshore_weighted": onsh,
                "solar_capture_rate":    (solar / spot * 100) if spot else 0,
                "offshore_capture_rate": (offsh / spot * 100) if spot else 0,
                "onshore_capture_rate":  (onsh  / spot * 100) if spot else 0,
            })
        if rows:
            supabase.table("dk_prices").upsert(rows, on_conflict="area,month").execute()

    for area in areas:
        for source_name, prod_dict in [
            ("solar", solar_prod[area]),
            ("offshore", offshore_prod[area]),
            ("onshore", onshore_prod[area]),
        ]:
            monthly_by_year = defaultdict(lambda: defaultdict(float))
            for dt, prod in prod_dict.items():
                monthly_by_year[dt.year][dt.month] += prod
            rows = []
            for year, months in monthly_by_year.items():
                for month, val in months.items():
                    if is_too_recent(year, month):
                        continue
                    rows.append({
                        "area": area, "source": source_name,
                        "year": year, "month": month, "value_mwh": val
                    })
            supabase.table("dk_production").upsert(rows, on_conflict="area,source,year,month").execute()

    print("DK data gemt.")

HYDRO_ZONES = {
    "Norge":   {"NO1": "10YNO-1--------2", "NO2": "10YNO-2--------T",
                "NO3": "10YNO-3--------J", "NO4": "10YNO-4--------9",
                "NO5": "10Y1001A1001A48H"},
    "Sverige": {"SE1": "10Y1001A1001A44P", "SE2": "10Y1001A1001A45N",
                "SE3": "10Y1001A1001A46L", "SE4": "10Y1001A1001A47J"},
}
HYDRO_PSR_TYPES = {"B11", "B12", "B10"}

def fetch_hydro_monthly_a75(eic_code, year, token):
    monthly = defaultdict(float)
    for month in range(1, 13):
        next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
        params = {
            "documentType": "A75", "processType": "A16",
            "in_Domain": eic_code,
            "periodStart": f"{year}{month:02d}010000",
            "periodEnd":   f"{next_year}{next_month:02d}010000",
            "securityToken": token,
        }
        for attempt in range(3):
            r = requests.get(ENTSOE_URL, params=params)
            if r.status_code == 200:
                break
            elif r.status_code in (503, 429):
                time.sleep(10 * (attempt + 1))
            else:
                break
        else:
            continue
        try:
            root = ET.fromstring(r.text)
        except ET.ParseError:
            continue
        ns = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
        for ts in root.findall(".//ns:TimeSeries", ns):
            psr_el = ts.find(".//ns:psrType", ns)
            if psr_el is None or psr_el.text not in HYDRO_PSR_TYPES:
                continue
            for period in ts.findall("ns:Period", ns):
                res_el = period.find("ns:resolution", ns)
                resolution = res_el.text if res_el is not None else "PT60M"
                for point in period.findall("ns:Point", ns):
                    qty_el = point.find("ns:quantity", ns)
                    if qty_el is None:
                        continue
                    try:
                        qty = float(qty_el.text)
                    except ValueError:
                        continue
                    if resolution == "PT15M":
                        qty /= 4
                    monthly[month] += qty
        time.sleep(1)
    return dict(monthly)

def collect_hydro_data():
    print("Henter hydro data...")
    rows = []
    for country, zones in HYDRO_ZONES.items():
        for zone, eic in zones.items():
            for year in fetch_years:
                print(f"  {zone} {year}...")
                monthly = fetch_hydro_monthly_a75(eic, year, ENTSOE_TOKEN)
                for month, val in monthly.items():
                    if is_too_recent(year, month):
                        continue
                    rows.append({
                        "country": country,
                        "zone": zone,
                        "year": year,
                        "month": month,
                        "value_mwh": val
                    })
    if rows:
        supabase.table("hydro_production").upsert(rows, on_conflict="country,zone,year,month").execute()
        print(f"Hydro data gemt ({len(rows)} rækker).")
    else:
        print("Ingen hydro data fundet – springes over.")

GAS_COUNTRIES = {
    "EU":        {"param": "continent", "code": "EU"},
    "Tyskland": {"param": "country",   "code": "de"},
    "Holland":  {"param": "country",   "code": "nl"},
}

def fetch_gas_storage_monthly(area_config, year, api_key):
    headers      = {"x-key": api_key}
    monthly_full = defaultdict(list)
    page         = 1
    while True:
        params = {
            area_config["param"]: area_config["code"],
            "from": f"{year}-01-01", "to": f"{year}-12-31",
            "page": page, "size": 300,
        }
        r = requests.get(AGSI_URL, headers=headers, params=params)
        if r.status_code != 200:
            break
        data      = r.json()
        records   = data.get("data", [])
        last_page = data.get("last_page", 1)
        for rec in records:
            try:
                dt       = datetime.strptime(rec.get("gasDayStart", ""), "%Y-%m-%d")
                full_val = rec.get("full")
                if full_val is not None:
                    monthly_full[dt.month].append(float(full_val))
            except (ValueError, TypeError):
                continue
        if page >= last_page:
            break
        page += 1
    return {m: sum(v) / len(v) for m, v in monthly_full.items() if v}

def collect_gas_data():
    print("Henter gas data...")
    rows = []
    for area_name, area_config in GAS_COUNTRIES.items():
        for year in fetch_years:
            print(f"  {area_name} {year}...")
            monthly = fetch_gas_storage_monthly(area_config, year, AGSI_KEY)
            for month, val in monthly.items():
                if is_too_recent(year, month):
                    continue
                rows.append({
                    "area": area_name, "year": year,
                    "month": month, "full_pct": val
                })
    if rows:
        supabase.table("gas_storage").upsert(rows, on_conflict="area,year,month").execute()

# ENTSO-E A68 installed capacity publiceres per kontrol-område, ikke per budzone.
# Vi bruger kontrol-område EIC'er og filtrerer PSR-typer der ikke findes i landet.
CAPACITY_COUNTRIES = {
    "Danmark": {
    "eic": "10Y1001A1001A65H",
    "allowed_psr": {"B01", "B04", "B05", "B06", "B10", "B12", "B13", "B14"},
    },
    
    "Norge": {
    "eic": "10YNO-0--------C",
    "allowed_psr": {"B01", "B04", "B09", "B10", "B11", "B12", "B13", "B14", "B17", "B18"},
    },
    
    "Sverige": {
    "eic": "10YSE-1--------K",
    "allowed_psr": {"B01", "B09", "B10", "B11", "B13", "B14", "B16", "B17", "B18", "B19", "B21"},
    },
    "Finland": {
    "eic": "10YFI-1--------U",
    "allowed_psr": {"B01", "B04", "B05", "B06", "B08", "B10", "B13", "B14", "B16", "B17", "B18", "B19", "B21"},
    },
    "Holland": {
    "eic": "10YNL----------L",
    "allowed_psr": {"B01", "B04", "B05", "B06", "B10", "B12", "B13", "B14", "B16", "B17", "B18", "B19", "B21"},
    },
    
    "Frankrig": {
    "eic": "10YFR-RTE------C",
    "allowed_psr": {"B01", "B04", "B05", "B06", "B09", "B10", "B11", "B12", "B13", "B14", "B15", "B16", "B18", "B19", "B20"},
    },
    
    "Tyskland": {
    "eic": "10Y1001A1001A83F",
    "allowed_psr": {"B01", "B02", "B03", "B04", "B05", "B06", "B09", "B10", "B11", "B12", "B13", "B14", "B15", "B17", "B18", "B19", "B21"},
    },
}

PSR_NAMES = {
    "B01": "Biomass", "B02": "Fossil Brown coal/Lignite", "B03": "Fossil Coal-derived gas",
    "B04": "Fossil Gas", "B05": "Fossil Hard coal", "B06": "Fossil Oil",
    "B07": "Fossil Oil shale", "B08": "Fossil Peat", "B09": "Hydro Pumped Storage",
    "B10": "Hydro Run-of-river and pondage", "B11": "Hydro Water Reservoir",
    "B12": "Wind Offshore", "B13": "Wind Onshore", "B14": "Solar",
    "B15": "Geothermal", "B16": "Nuclear", "B17": "Other renewable",
    "B18": "Waste", "B19": "Other", "B20": "Marine", "B21": "Energy storage",
}

def fetch_capacity_for_country(eic, year, allowed_psr):
    """Henter installed capacity for ét kontrol-område og returnerer {psr: max_mw},
    filtreret til kun de PSR-typer der er relevante for landet."""
    params = {
        "documentType": "A68", "processType": "A33",
        "in_Domain": eic,
        "periodStart": f"{year}01010000",
        "periodEnd":   f"{year}12312300",
        "securityToken": ENTSOE_TOKEN,
    }
    for attempt in range(3):
        r = requests.get(ENTSOE_URL, params=params, timeout=30)
        if r.status_code == 200:
            break
        elif r.status_code in (503, 429):
            time.sleep(10 * (attempt + 1))
        else:
            print(f"    ENTSOE fejl {r.status_code} for {eic} {year}")
            return {}
    else:
        return {}

    if "No matching data found" in r.text:
        return {}

    try:
        root = ET.fromstring(r.text)
    except ET.ParseError:
        return {}

    import re
    matches = re.findall(r'<psrType>(.*?)</psrType>.*?<quantity>(.*?)</quantity>', r.text, re.DOTALL)
    print(f"    Rå PSR matches: {matches[:10]}")

    ns = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
    seen = {}
    for ts in root.findall(".//ns:TimeSeries", ns):
        psr_el = ts.find(".//ns:psrType", ns)
        if psr_el is None:
            continue
        psr = psr_el.text
        # Filtrer PSR-typer der ikke hører til landet
        if psr not in allowed_psr:
            continue
        for period in ts.findall("ns:Period", ns):
            for point in period.findall("ns:Point", ns):
                qty_el = point.find("ns:quantity", ns)
                if qty_el is None:
                    continue
                try:
                    qty = float(qty_el.text)
                except ValueError:
                    continue
                if qty > seen.get(psr, 0):
                    seen[psr] = qty
    print(f"    PSR data: {seen}")
    return seen

def collect_capacity_data():
    print("Henter installed capacity...")
    rows = []
    for country, config in CAPACITY_COUNTRIES.items():
        if country != "Frankrig":
            continue
        eic         = config["eic"]
        allowed_psr = config["allowed_psr"]
        for year in [2024]:
            print(f"  {country} {year}...")
            data = fetch_capacity_for_country(eic, year, allowed_psr)
            if not data:
                print(f"    Ingen data for {country} {year}")
            for psr, mw in data.items():
                rows.append({
                    "country": country, "psr_type": psr,
                    "psr_name": PSR_NAMES.get(psr, psr),
                    "year": year, "value_mw": mw
                })
            time.sleep(1)

    if rows:
        supabase.table("installed_capacity").upsert(rows, on_conflict="country,psr_type,year").execute()
        print(f"Capacity data gemt ({len(rows)} rækker).")

CONSUMPTION_ZONES = {
    "DK1":      "10YDK-1--------W",
    "DK2":      "10YDK-2--------M",
    "Tyskland": "10Y1001A1001A83F",
}

def fetch_consumption_monthly(eic_code, year, token):
    monthly = defaultdict(list)
    hourly  = defaultdict(list)
    params = {
        "documentType": "A65", "processType": "A16",
        "outBiddingZone_Domain": eic_code,
        "periodStart": f"{year}01010000", "periodEnd": f"{year}12312300",
        "securityToken": token,
    }
    for attempt in range(3):
        r = requests.get(ENTSOE_URL, params=params, timeout=60)
        if r.status_code == 200:
            break
        elif r.status_code in (503, 429):
            time.sleep(15 * (attempt + 1))
        else:
            return {}, {}
    else:
        return {}, {}

    if "No matching data found" in r.text:
        return {}, {}
    try:
        root = ET.fromstring(r.text)
    except ET.ParseError:
        return {}, {}

    ns_uri = root.tag.split("}")[0][1:] if root.tag.startswith("{") else ""
    prefix = f"{{{ns_uri}}}" if ns_uri else ""

    for ts in root.findall(f".//{prefix}TimeSeries"):
        for period in ts.findall(f"{prefix}Period"):
            start_el = period.find(f"{prefix}timeInterval/{prefix}start")
            res_el   = period.find(f"{prefix}resolution")
            if start_el is None or res_el is None:
                continue
            start_dt   = datetime.strptime(start_el.text, "%Y-%m-%dT%H:%MZ")
            resolution = res_el.text
            for point in period.findall(f"{prefix}Point"):
                pos_el = point.find(f"{prefix}position")
                qty_el = point.find(f"{prefix}quantity")
                if pos_el is None or qty_el is None:
                    continue
                try:
                    pos = int(pos_el.text)
                    qty = float(qty_el.text)
                except:
                    continue
                offset = (pos - 1) * (15 if resolution == "PT15M" else 60)
                dt = start_dt + timedelta(minutes=offset)
                monthly[dt.month].append(qty)
                hourly[dt.hour].append(qty)

    m_avg = {m: sum(v) / len(v) for m, v in monthly.items() if v}
    h_avg = {h: sum(v) / len(v) for h, v in hourly.items() if v}
    return m_avg, h_avg

def collect_consumption_data():
    print("Henter forbrug...")
    m_rows = []
    h_rows = []
    for zone, eic in CONSUMPTION_ZONES.items():
        for year in fetch_years:
            print(f"  {zone} {year}...")
            monthly, hourly = fetch_consumption_monthly(eic, year, ENTSOE_TOKEN)
            for month, val in monthly.items():
                if is_too_recent(year, month):
                    continue
                m_rows.append({"zone": zone, "year": year, "month": month, "value_mwh": val})
            for hour, val in hourly.items():
                h_rows.append({"zone": zone, "year": year, "hour": hour, "value_mwh": val})
            time.sleep(1)

    if m_rows:
        supabase.table("consumption").upsert(m_rows, on_conflict="zone,year,month").execute()
    if h_rows:
        supabase.table("consumption_hourly").upsert(h_rows, on_conflict="zone,year,hour").execute()
    print("Forbrugsdata gemt.")

def collect_all():
    print(f"\n{'='*40}\nStart: {datetime.now()}\n{'='*40}")
    collect_capacity_data()
    print(f"\nSlut: {datetime.now()}")

if __name__ == "__main__":
    collect_all()
