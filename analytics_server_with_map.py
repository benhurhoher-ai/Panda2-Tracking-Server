from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import sqlite3
import time
import json

app = Flask(__name__)
CORS(app)

DB_PATH = "locations.db"

def get_db():
    return sqlite3.connect(DB_PATH)

def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS device_config (
        device TEXT PRIMARY KEY,
        tracking_enabled INTEGER NOT NULL DEFAULT 1,
        interval_sec INTEGER NOT NULL DEFAULT 30,
        updated_at INTEGER
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS location_points (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device TEXT NOT NULL,
        lat REAL NOT NULL,
        lon REAL NOT NULL,
        accuracy REAL,
        recorded_at INTEGER NOT NULL
    )
    """)

    conn.commit()
    conn.close()

init_db()

def ensure_device_config(device):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO device_config (device, tracking_enabled, interval_sec, updated_at)
        VALUES (?, 1, 30, ?)
    """, (device, int(time.time() * 1000)))
    conn.commit()
    conn.close()

def get_device_config(device):
    ensure_device_config(device)
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT device, tracking_enabled, interval_sec, updated_at
        FROM device_config
        WHERE device = ?
    """, (device,))
    row = cur.fetchone()
    conn.close()

    return {
        "device": row[0],
        "tracking_enabled": bool(row[1]),
        "interval_sec": row[2],
        "updated_at": row[3],
    }

@app.route("/")
def home():
    return jsonify({
        "name": "Panda2 Tracking Server",
        "status": "ok",
        "endpoints": [
            "GET /config/<device>",
            "POST /location",
            "GET /points?device=panda2",
            "GET /latest/<device>",
            "GET /devices",
            "GET /map?device=panda2"
        ]
    })

@app.route("/config/<device>", methods=["GET"])
def api_get_config(device):
    return jsonify(get_device_config(device))
    @app.route("/movement/<device>")
def api_movement(device):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT lat, lon, recorded_at
        FROM location_points
        WHERE device = ?
        ORDER BY recorded_at DESC
        LIMIT 2
    """, (device,))
    rows = cur.fetchall()
    conn.close()

    if len(rows) < 2:
        return jsonify({"device": device, "moving": False, "distance_m": 0})

    lat1, lon1, ts1 = rows[0]
    lat2, lon2, ts2 = rows[1]

    from math import radians, sin, cos, sqrt, atan2
    r = 6371000
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    dist = 2 * r * atan2(sqrt(a), sqrt(1 - a))

    return jsonify({
        "device": device,
        "moving": dist > 20,
        "distance_m": round(dist, 1),
        "latest_ts": ts1
    })

@app.route("/location", methods=["POST"])
def api_location():
    data = request.json or {}
    device = data.get("device")
    lat = data.get("lat")
    lon = data.get("lon")
    accuracy = data.get("accuracy", 0)

    if not device or lat is None or lon is None:
        return jsonify({"error": "missing data"}), 400

    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO location_points (device, lat, lon, accuracy, recorded_at)
        VALUES (?, ?, ?, ?, ?)
    """, (device, float(lat), float(lon), float(accuracy or 0), int(time.time() * 1000)))
    conn.commit()
    conn.close()

    return jsonify({"status": "ok"})

@app.route("/devices")
def api_devices():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT device FROM location_points ORDER BY device")
    rows = cur.fetchall()
    conn.close()
    return jsonify({"devices": [r[0] for r in rows]})

@app.route("/points")
def api_points():
    device = request.args.get("device")
    if not device:
        return jsonify({"error": "missing device"}), 400

    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT lat, lon, accuracy, recorded_at
        FROM location_points
        WHERE device = ?
        ORDER BY recorded_at DESC
        LIMIT 500
    """, (device,))
    rows = cur.fetchall()
    conn.close()

    points = [
        {
            "lat": r[0],
            "lon": r[1],
            "accuracy": r[2],
            "timestamp": r[3]
        }
        for r in rows
    ]
    return jsonify({"device": device, "count": len(points), "points": points})

@app.route("/latest/<device>")
def api_latest(device):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT lat, lon, accuracy, recorded_at
        FROM location_points
        WHERE device = ?
        ORDER BY recorded_at DESC
        LIMIT 1
    """, (device,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return jsonify({"error": "no data"}), 404

    return jsonify({
        "device": device,
        "lat": row[0],
        "lon": row[1],
        "accuracy": row[2],
        "timestamp": row[3]
    })

@app.route("/map")
def api_map():
    device = request.args.get("device", "panda2")
    html = f"""<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Panda2 Live-Karte</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet/dist/leaflet.css" />
  <style>
    html, body {{
      margin: 0;
      padding: 0;
      height: 100%;
      font-family: Arial, sans-serif;
      background: #f8fafc;
    }}
    #topbar {{
      padding: 10px 14px;
      background: #111827;
      color: white;
      font-size: 14px;
    }}
    #map {{
      height: calc(100% - 48px);
      width: 100%;
    }}
  </style>
</head>
<body>
  <div id="topbar">Panda2 Live-Karte – Gerät: <b>{device}</b> – Aktualisierung alle 10 Sekunden</div>
  <div id="map"></div>

  <script src="https://unpkg.com/leaflet/dist/leaflet.js"></script>
  <script>
    const device = {json.dumps(device)};
    const map = L.map('map').setView([51.0, 10.0], 6);
    L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
      maxZoom: 19,
      attribution: '&copy; OpenStreetMap'
    }}).addTo(map);

    let marker = null;
    let polyline = null;

    async function loadData() {{
      try {{
        const latestRes = await fetch(`/latest/${{device}}`);
        if (latestRes.ok) {{
          const latest = await latestRes.json();
          const pos = [latest.lat, latest.lon];

          if (!marker) {{
            marker = L.marker(pos).addTo(map);
          }} else {{
            marker.setLatLng(pos);
          }}

          marker.bindPopup(
            `<b>${{latest.device}}</b><br>` +
            `Lat: ${{latest.lat}}<br>` +
            `Lon: ${{latest.lon}}<br>` +
            `Accuracy: ${{latest.accuracy}}<br>` +
            `Zeit: ${{new Date(latest.timestamp).toLocaleString()}}`
          );

          map.setView(pos, 16);
        }}

        const pointsRes = await fetch(`/points?device=${{encodeURIComponent(device)}}`);
        if (pointsRes.ok) {{
          const data = await pointsRes.json();
          const coords = data.points.slice().reverse().map(p => [p.lat, p.lon]);
          if (coords.length > 1) {{
            if (polyline) {{
              polyline.setLatLngs(coords);
            }} else {{
              polyline = L.polyline(coords).addTo(map);
            }}
          }}
        }}
      }} catch (e) {{
        console.error(e);
      }}
    }}

    loadData();
    setInterval(loadData, 10000);
  </script>
</body>
</html>"""
    return Response(html, mimetype="text/html")

import os

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
