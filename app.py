from flask import Flask, jsonify
from flask_cors import CORS
from supabase import create_client
import os

app = Flask(__name__)
CORS(app)

supabase = create_client(
    os.environ.get("SUPABASE_URL"),
    os.environ.get("SUPABASE_KEY")
)

@app.route("/api/dk-prices/<area>")
def dk_prices(area):
    r = supabase.table("dk_prices").select("*").eq("area", area).order("month").execute()
    return jsonify(r.data)

@app.route("/api/dk-production/<area>/<source>")
def dk_production(area, source):
    r = supabase.table("dk_production").select("*")\
        .eq("area", area).eq("source", source)\
        .order("year").order("month").execute()
    return jsonify(r.data)

@app.route("/api/hydro/<country>/<zone>")
def hydro(country, zone):
    r = supabase.table("hydro_production").select("*")\
        .eq("country", country).eq("zone", zone)\
        .order("year").order("month").execute()
    return jsonify(r.data)

@app.route("/api/gas/<area>")
def gas(area):
    r = supabase.table("gas_storage").select("*")\
        .eq("area", area).order("year").order("month").execute()
    return jsonify(r.data)

@app.route("/api/capacity/<country>")
def capacity(country):
    r = supabase.table("installed_capacity").select("*")\
        .eq("country", country).order("year").execute()
    return jsonify(r.data)

@app.route("/api/refresh", methods=["POST"])
def refresh():
    from collector import collect_all
    import threading
    threading.Thread(target=collect_all).start()
    return jsonify({"status": "started"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
```

**`Procfile`** (til Railway):
```
web: gunicorn app:app --bind 0.0.0.0:$PORT