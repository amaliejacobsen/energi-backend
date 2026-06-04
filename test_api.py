import requests

r = requests.get(
    "https://api.energidataservice.dk/dataset/ElectricityBalanceNonv",
    params={
        "filter": '{"PriceArea":"DK1"}',
        "start": "2026-06-03T00:00",
        "limit": 5,
        "sort": "HourUTC desc",
    },
    timeout=30
)
print("Status:", r.status_code)
print("Response:", r.text[:500])
