import requests
import xml.etree.ElementTree as ET
import statistics
import time
from datetime import datetime
from collections import defaultdict
from supabase import create_client
import os

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase     = create_client(SUPABASE_URL, SUPABASE_KEY)

ENTSOE_TOKEN = os.environ.get("ENTSOE_TOKEN", "138899c3-59b3-48ef-9dfd-03406794210d")
ENTSOE_URL   = "https://web-api.tp.entsoe.eu/api"
AGSI_KEY     = os.environ.get("AGSI_KEY", "12a5ed2eb1b9d3f2091abd2213758ec2")
AGSI_URL     = "https://agsi.gie.eu/api"

end          = datetime.today().strftime("%Y-%m-%d")
current_year = datetime.today().year
current_month = datetime.today().month
last_full_month = current_month - 1
fetch_years  = list(range(2020, current_year + 1))
areas        = ["DK1", "DK2"]

# ---- Genbrugte hjælpefunktioner fra dit eksisterende script ----
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

# ---- DK priser og produktion ----
def collect_dk_data():
    print("Henter DK data...")
    hourly_prices = {area: {} for area in areas}
    solar_prod    = {area: {} for area in areas}
    offshore_prod = {area: {} for area in areas}
    onshore_prod  = {area: {} for area in areas}

    for area in areas:
        r = requests.get("https://api.energidataservice.dk/dataset/Elspotprices", params={
            "start": "2020-01-01", "end": "2025-09-30",
            "filter": f'{{"PriceArea":"{area}"}}', "limit": 100000
        })
        for rec in r.json().get("records", []):
            dt = datetime.fromisoformat(rec["HourDK"])
            hourly_prices[area][dt] = rec["SpotPriceDKK"]

        r = requests.get("https://api.energidataservice.dk/dataset/DayAheadPrices", params={
            "start": "2025-10-01", "end": end,
            "filter": f'{{"PriceArea":"{area}"}}', "limit": 100000
        })
        quarter_prices = defaultdict(list)
        for rec in r.json().get("records", []):
            dt   = datetime.fromisoformat(rec["TimeDK"])
            hour = dt.replace(minute=0, second=0, microsecond=0)
            quarter_prices[hour].append(rec["DayAheadPriceDKK"])
        for hour, prices in quarter_prices.items():
            hourly_prices[area][hour] = sum(prices) / len(prices)

        r = requests.get("https://api.energidataservice.dk/dataset/ProductionConsumptionSettlement", params={
            "start": "2020-01-01", "end": end,
            "filter": f'{{"PriceArea":"{area}"}}', "limit": 100000
        })
        for rec in r.json().get("records", []):
            dt = datetime.fromisoformat(rec["HourDK"])
            solar_prod[area][dt] = (
                rec.get("SolarPowerLt10kW_MWh", 0) +
                rec.get("SolarPowerGe10Lt40kW_MWh", 0) +
                rec.get("SolarPowerGe40kW_MWh", 0)
            )
            offshore_prod[area][dt] = (
                rec.get("OffshoreWindLt100MW_MWh", 0) +
                rec.get("OffshoreWindGe100MW_MWh", 0)
            )
            onshore_prod[area][dt] = (
                rec.get("OnshoreWindLt50kW_MWh", 0) +
                rec.get("OnshoreWindGe50kW_MWh", 0)
            )

    # Gem priser og capture rates
    for area in areas:
        avg_prices   = monthly_avg_prices(hourly_prices[area])
        avg_solar    = monthly_weighted(hourly_prices[area], solar_prod[area])
        avg_offshore = monthly_weighted(hourly_prices[area], offshore_prod[area])
        avg_onshore  = monthly_weighted(hourly_prices[area], onshore_prod[area])

        all_months = sorted(set(avg_prices) | set(avg_solar) | set(avg_offshore) | set(avg_onshore))
        rows = []
        for month in all_months:
            spot  = avg_prices.get(month, 0)
            solar = avg_solar.get(month, 0)
            offsh = avg_offshore.get(month, 0)
            onsh  = avg_onshore.get(month, 0)
            rows.append({
                "area": area, "month": month,
                "spot_price": spot,
                "solar_weighted": solar,
                "offshore_weighted": offsh,
                "onshore_weighted": onsh,
                "solar_capture_rate":    (solar / spot * 100) if spot else 0,
                "offshore_capture_rate": (offsh / spot * 100) if spot else 0,
                "onshore_capture_rate":  (onsh  / spot * 100) if spot else 0,
            })
        supabase.table("dk_prices").upsert(rows, on_conflict="area,month").execute()

    # Gem produktion
    for area in areas:
        for source_name, prod_dict in [
            ("solar", solar_prod[area]),
            ("offshore", offshore_prod[area]),
            ("onshore", onshore_prod[area]),
        ]:
            monthly_by_year = defaultdict(lambda: defaultdict(float))
            for dt, prod in prod_dict.items():
                if dt.year == current_year and dt.month > last_full_month:
                    continue
                monthly_by_year[dt.year][dt.month] += prod

            rows = []
            for year, months in monthly_by_year.items():
                for month, val in months.items():
                    rows.append({
                        "area": area, "source": source_name,
                        "year": year, "month": month, "value_mwh": val
                    })
            supabase.table("dk_production").upsert(rows, on_conflict="area,source,year,month").execute()

    print("DK data gemt.")

# ---- Hydro ----
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
                    if year == current_year and month > last_full_month:
                        continue
                    rows.append({
                        "country": country, "zone": zone,
                        "year": year, "month": month, "value_mwh": val
                    })
    supabase.table("hydro_production").upsert(rows, on_conflict="country,zone,year,month").execute()
    print("Hydro data gemt.")

# ---- Gas storage ----
GAS_COUNTRIES = {
    "EU":       {"param": "continent", "code": "EU"},
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
                if year == current_year and month > last_full_month:
                    continue
                rows.append({
                    "area": area_name, "year": year,
                    "month": month, "full_pct": val
                })
    supabase.table("gas_storage").upsert(rows, on_conflict="area,year,month").execute()
    print("Gas data gemt.")

# ---- Installed capacity ----
CAPACITY_COUNTRIES = {
    "Danmark":  "10Y1001A1001A65H",
    "Norge":    "10YNO-0--------C",
    "Sverige":  "10YSE-1--------K",
    "Finland":  "10YFI-1--------U",
    "Holland":  "10YNL----------L",
    "Frankrig": "10YFR-RTE------C",
}

PSR_NAMES = {
    "B01": "Biomasse",       "B02": "Brun kul",
    "B03": "Gas (fossil)",   "B04": "Gas turbine",
    "B05": "Kul (fossil)",   "B06": "Olie",
    "B09": "Vandkraft (pumped)", "B10": "Vandkraft (run-of-river)",
    "B11": "Vandkraft (reservoir)", "B12": "Havvind",
    "B13": "Landvind",       "B14": "Sol",
    "B16": "Atomkraft",      "B17": "Andet VE",
    "B18": "Affald",         "B19": "Andet ikke-VE",
}

def collect_capacity_data():
    print("Henter installed capacity...")
    rows = []
    for country, eic in CAPACITY_COUNTRIES.items():
        for year in range(2020, current_year + 1):
            params = {
                "documentType": "A68", "processType": "A33",
                "in_Domain": eic,
                "periodStart": f"{year}01010000",
                "periodEnd":   f"{year}12312300",
                "securityToken": ENTSOE_TOKEN,
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
            if "No matching data found" in r.text:
                continue
            try:
                root = ET.fromstring(r.text)
            except ET.ParseError:
                continue
            ns = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
            seen = {}
            for ts in root.findall(".//ns:TimeSeries", ns):
                psr_el = ts.find(".//ns:psrType", ns)
                if psr_el is None:
                    continue
                psr = psr_el.text
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
            for psr, mw in seen.items():
                rows.append({
                    "country": country, "psr_type": psr,
                    "psr_name": PSR_NAMES.get(psr, psr),
                    "year": year, "value_mw": mw
                })
            time.sleep(1)
    supabase.table("installed_capacity").upsert(rows, on_conflict="country,psr_type,year").execute()
    print("Installed capacity gemt.")

def collect_all():
    print(f"\n{'='*40}\nDataindsamling startet: {datetime.now()}\n{'='*40}")
    collect_dk_data()
    collect_gas_data()
    collect_hydro_data()
    collect_capacity_data()
    print(f"\nAlt data gemt: {datetime.now()}")

if __name__ == "__main__":

    # ---- Forbrug ----
CONSUMPTION_ZONES = {
    "DK1":      "10YDK-1--------W",
    "DK2":      "10YDK-2--------M",
    "Tyskland": "10Y1001A1001A83F",
}

def fetch_consumption_monthly(eic_code, year, token):
    monthly = defaultdict(list)
    params = {
        "documentType":          "A65",
        "processType":           "A16",
        "outBiddingZone_Domain": eic_code,
        "periodStart":           f"{year}01010000",
        "periodEnd":             f"{year}12312300",
        "securityToken":         token,
    }
    for attempt in range(3):
        r = requests.get(ENTSOE_URL, params=params, timeout=60)
        if r.status_code == 200:
            break
        elif r.status_code in (503, 429):
            time.sleep(15 * (attempt + 1))
        else:
            return {}
    else:
        return {}

    if "No matching data found" in r.text:
        return {}

    try:
        root = ET.fromstring(r.text)
    except ET.ParseError:
        return {}

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
                except (ValueError, TypeError):
                    continue
                if resolution == "PT60M":
                    dt = start_dt + __import__('datetime').timedelta(hours=pos - 1)
                elif resolution == "PT15M":
                    dt = start_dt + __import__('datetime').timedelta(minutes=(pos - 1) * 15)
                else:
                    continue
                monthly[dt.month].append(qty)

    return {m: sum(v) / len(v) for m, v in monthly.items() if v}

def collect_consumption_data():
    print("Henter forbrug...")
    rows = []
    for zone, eic in CONSUMPTION_ZONES.items():
        for year in fetch_years:
            print(f"  {zone} {year}...")
            monthly = fetch_consumption_monthly(eic, year, ENTSOE_TOKEN)
            for month, val in monthly.items():
                if year == current_year and month > last_full_month:
                    continue
                rows.append({
                    "zone": zone, "year": year,
                    "month": month, "value_mwh": val
                })
            time.sleep(1)
    if rows:
        supabase.table("consumption").upsert(
            rows, on_conflict="zone,year,month"
        ).execute()
    print("Forbrug gemt.")
    collect_all()
