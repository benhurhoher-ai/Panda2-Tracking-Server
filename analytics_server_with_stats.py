# ===== PANDA2 COMPLETE SERVER =====
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import sqlite3, time, os, requests
from math import radians, sin, cos, sqrt, atan2

app = Flask(__name__)
CORS(app)

DB_PATH = "locations.db"

def get_db():
    return sqlite3.connect(DB_PATH)

def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS location_points (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device TEXT,
        lat REAL,
        lon REAL,
        recorded_at INTEGER
    )
    """)

    conn.commit()
    conn.close()

init_db()

# ===== HELPERS =====
def haversine_m(lat1, lon1, lat2, lon2):
    r = 6371000
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
    return 2*r*atan2(sqrt(a), sqrt(1-a))

def fmt_ts(ts):
    return time.strftime("%d.%m.%Y %H:%M", time.localtime(ts/1000))

def fmt_duration(sec):
    sec = int(sec)
    h = sec // 3600
    m = (sec % 3600)//60
    return f"{h}h {m}min" if h else f"{m}min"

def get_address(lat, lon):
    try:
        url = f"https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json"
        r = requests.get(url, headers={"User-Agent":"panda"})
        return r.json().get("display_name","Unbekannt")
    except:
        return "Unbekannt"

# ===== DATA =====
def get_points(device):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT lat, lon, recorded_at FROM location_points WHERE device=? ORDER BY recorded_at", (device,))
    rows = cur.fetchall()
    conn.close()
    return rows

# ===== STAYS =====
def compute_stays(device):
    rows = get_points(device)
    if not rows: return []

    stays=[]
    cluster=[rows[0]]

    for row in rows[1:]:
        prev = cluster[-1]
        dist = haversine_m(prev[0],prev[1],row[0],row[1])

        if dist < 80:
            cluster.append(row)
        else:
            start = cluster[0][2]
            end = cluster[-1][2]
            dur = (end-start)//1000

            if dur > 60:
                lat = sum(p[0] for p in cluster)/len(cluster)
                lon = sum(p[1] for p in cluster)/len(cluster)
                addr = get_address(lat,lon)

                stays.append({
                    "address": addr,
                    "arrival": fmt_ts(start),
                    "departure": fmt_ts(end),
                    "duration": fmt_duration(dur),
                    "lat": lat,
                    "lon": lon
                })
            cluster=[row]

    return stays

# ===== ROUTES =====

@app.route("/")
def home():
    return {
        "status": "ok",
        "message": "Panda Server läuft",
        "links": {
            "dashboard": "/dashboard?device=panda2",
            "map": "/map?device=panda2",
            "stays": "/stays?device=panda2"
        }
    }
    
@app.route("/location", methods=["POST"])
def loc():
    d=request.json
    conn=get_db()
    cur=conn.cursor()
    cur.execute("INSERT INTO location_points(device,lat,lon,recorded_at) VALUES(?,?,?,?)",
                (d["device"],d["lat"],d["lon"],int(time.time()*1000)))
    conn.commit()
    conn.close()
    return {"ok":True}

@app.route("/stays")
def stays():
    device=request.args.get("device")
    return jsonify(compute_stays(device))

# ===== DASHBOARD =====
@app.route("/dashboard")
def api_dashboard():
    device = request.args.get("device", "panda2")
    period = request.args.get("period", "all")
    start_ts, end_ts = day_range(period)

    stays = compute_stays(device, start_ts=start_ts, end_ts=end_ts)
    places = summarize_places(device, start_ts=start_ts, end_ts=end_ts)

    stays_rows = ""
    for s in reversed(stays):
        stays_rows += f"""
        <tr>
            <td>{s.get("address", s["label"])}</td>
            <td>{s["arrival"]}</td>
            <td>{s["departure"]}</td>
            <td>{s["duration_human"]}</td>
        </tr>
        """

    places_rows = ""
    for p in places:
        places_rows += f"""
        <tr>
            <td>{p.get("kind", "Ort")}</td>
            <td>{p["label"]}</td>
            <td>{p["visit_count"]}</td>
            <td>{p["total_duration_human"]}</td>
        </tr>
        """

    html = """
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="UTF-8">
      <title>Panda Statistik</title>
      <style>
        body { font-family: Arial; margin:20px; }
        table { border-collapse: collapse; width:100%; }
        th, td { border:1px solid #ccc; padding:8px; }
      </style>
    </head>
    <body>

    <h1>Aufenthalte</h1>
    <table>
    <tr><th>Adresse</th><th>Ankunft</th><th>Gehen</th><th>Dauer</th></tr>
    """ + stays_rows + """
    </table>

    <h1>Orte</h1>
    <table>
    <tr><th>Typ</th><th>Ort</th><th>Besuche</th><th>Dauer</th></tr>
    """ + places_rows + """
    </table>

    </body>
    </html>
    """

    return Response(html, mimetype="text/html")

@app.route("/map")
def api_map():
    device = request.args.get("device", "panda2")

    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Panda Karte</title>
        <link rel="stylesheet" href="https://unpkg.com/leaflet/dist/leaflet.css"/>
        <style>
            body { margin:0; }
            #map { height:100vh; }
        </style>
    </head>
    <body>
        <div id="map"></div>

        <script src="https://unpkg.com/leaflet/dist/leaflet.js"></script>
        <script>
            var device = '__DEVICE__';

            var map = L.map('map').setView([51,10],6);

            L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png')
            .addTo(map);

            fetch('/stays?device=' + device)
            .then(r => r.json())
            .then(data => {
                (data.stays || []).forEach(p => {
                    L.marker([p.lat, p.lon]).addTo(map)
                    .bindPopup(p.address);
                });
            });
        </script>
    </body>
    </html>
    """

    html = html.replace("__DEVICE__", device)
    return Response(html, mimetype="text/html")

# run via gunicorn
