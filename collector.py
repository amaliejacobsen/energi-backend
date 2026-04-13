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

end           = datetime.today().strftime("%Y-%m-%d")
current_year  = datetime.today().year
current_month = datetime.today().month

# RETTELSE 1: Vi tillader nu at hente data for den nuværende måned
last_full_month = current_month 

# RETTELSE 2: Vi henter kun 2026 for at gøre scriptet hurtigt nok til automatisk kørsel
fetch_years   = [current_year] 
areas         = ["DK1", "DK2"]

# ---- Hjælpefunktioner ----
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
        # RETTELSE 3: Henter priser helt frem til 'end' (i dag)
        r = requests.get("https://api.energidataservice.dk/dataset/Elspotprices", params={
            "start": "2024-01-01", "end": end,
            "filter": f'{{"PriceArea":"{area}"}}', "limit": 100000
        })
        for rec in r.json().get("records", []):
            dt = datetime.fromisoformat(rec["HourDK"].replace('Z', '+00:00'))
            hourly_prices[area][dt] = rec["SpotPriceDKK"]

        # Produktion
        r = requests.get("https://api.energidataservice.dk/dataset/ProductionConsumptionSettlement", params={
            "start": "2024-01-01", "end": end,
            "filter": f'{{"PriceArea":"{area}"}}', "limit": 100000
        })
        for rec in r.json().get("records", []):
            dt = datetime.fromisoformat(rec["HourDK"].replace('Z', '+00:00'))
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

    # Gem til database
    for area in areas:
        avg_prices   = monthly_avg_prices(hourly_prices[area])
        avg_solar    = monthly_weighted(hourly_prices[area], solar_prod[area])
        avg_offshore = monthly_weighted(hourly_prices[area], offshore_prod[area])
        avg_onshore  = monthly_weighted(hourly_prices[area], onshore_prod[area])

        all_months = sorted(set(avg_prices) | set(avg_solar) | set(avg_offshore) | set(avg_onshore))
        rows = []
        for month_key in all_months:
            spot  = avg_prices.get(month_key, 0)
            solar = avg_solar.get(month_key, 0)
            offsh = avg_offshore.get(month_key, 0)
            onsh  = avg_onshore.get(month_key, 0)
            
            if spot == 0: continue
            
            # Vi sikrer os at 'month' er i formatet 'YYYY-MM' (f.eks. '2026-04')
            # Hvis month_key allerede er en streng som '2026-04', så gemmes den direkte.
            rows.append({
                "area": area, 
                "month": str(month_key), 
                "spot_price": spot,
                "solar_weighted": solar,
                "offshore_weighted": offsh,
                "onshore_weighted": onsh,
                "solar_capture_rate":    (solar / spot * 100) if spot else 0,
                "offshore_capture_rate": (offsh / spot * 100) if spot else 0,
                "onshore_capture_rate":  (onsh  / spot * 100) if spot else 0,
            })
        
        supabase.table("dk_prices").upsert(rows, on_conflict="area,month").execute()

    print("DK data gemt.")

# ---- Hydro ----
HYDRO_ZONES = {
    "Norge":   {"NO1": "10YNO-1--------2", "NO2": "10YNO-2--------T"},
    "Sverige": {"SE3": "10Y1001A1001A46L"},
}
HYDRO_PSR_TYPES = {"B11", "B12", "B10"}

def fetch_hydro_monthly_a75(eic_code, year, token):
    monthly = defaultdict(float)
    # Vi henter kun data for de måneder der er gået i år
    for month in range(1, current_month + 1):
        next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
        params = {
            "documentType": "A75", "processType": "A16",
            "in_Domain": eic_code,
            "periodStart": f"{year}{month:02d}010000",
            "periodEnd":   f"{next_year}{next_month:02d}010000",
            "securityToken": token,
        }
        r = requests.get(ENTSOE_URL, params=params)
        if r.status_code == 200:
            try:
                root = ET.fromstring(r.text)
                ns = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
                for ts in root.findall(".//ns:TimeSeries", ns):
                    for period in ts.findall("ns:Period", ns):
                        for point in period.findall("ns:Point", ns):
                            qty_el = point.find("ns:quantity", ns)
                            if qty_el is not None:
                                monthly[month] += float(qty_el.text)
            except: continue
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
                    rows.append({
                        "country": country, "zone": zone,
                        "year": year, "month": month, "value_mwh": val
                    })
    if rows:
        supabase.table("hydro_production").upsert(rows, on_conflict="country,zone,year,month").execute()
    print("Hydro data gemt.")

# ---- Gas storage ----
GAS_COUNTRIES = {
    "EU":       {"param": "continent", "code": "EU"},
    "Tyskland": {"param": "country",   "code": "de"},
}

def fetch_gas_storage_monthly(area_config, year, api_key):
    headers      = {"x-key": api_key}
    monthly_full = defaultdict(list)
    params = {
        area_config["param"]: area_config["code"],
        "from": f"{year}-01-01", "to": end,
    }
    r = requests.get(AGSI_URL, headers=headers, params=params)
    if r.status_code == 200:
        for rec in r.json().get("data", []):
            try:
                dt = datetime.strptime(rec.get("gasDayStart", ""), "%Y-%m-%d")
                monthly_full[dt.month].append(float(rec["full"]))
            except: continue
    return {m: sum(v) / len(v) for m, v in monthly_full.items() if v}

def collect_gas_data():
    print("Henter gas data...")
    rows = []
    for area_name, area_config in GAS_COUNTRIES.items():
        for year in fetch_years:
            print(f"  {area_name} {year}...")
            monthly = fetch_gas_storage_monthly(area_config, year, AGSI_KEY)
            for month, val in monthly.items():
                rows.append({
                    "area": area_name, "year": year,
                    "month": month, "full_pct": val
                })
    if rows:
        supabase.table("gas_storage").upsert(rows, on_conflict="area,year,month").execute()
    print("Gas data gemt.")

def collect_all():
    print(f"\n{'='*40}\nDataindsamling startet: {datetime.now()}\n{'='*40}")
    collect_dk_data()
    collect_gas_data()
    collect_hydro_data()
    print(f"\nAlt data gemt: {datetime.now()}")

if __name__ == "__main__":
    collect_all()
