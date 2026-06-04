import requests

r = requests.get(
    "https://api.energidataservice.dk/dataset/ElectricityBalanceNonv",
    params={
        "filter": '{"PriceArea":"DK1"}',
        "limit": 1,
        "sort": "HourUTC desc",
    },
    timeout=30
)
print("Status:", r.status_code)
print("Response:", r.text[:500])
