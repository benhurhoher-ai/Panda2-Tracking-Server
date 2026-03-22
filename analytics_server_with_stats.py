# ===== PANDA2 COMPLETE SERVER =====
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import sqlite3, time, os, requests
from math import radians, sin, cos, sqrt, atan2

import smtplib
from email.mime.text import MIMEText

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
        url = f"https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json&addressdetails=1"
        headers = {"User-Agent": "panda2-tracker"}
        res = requests.get(url, headers=headers, timeout=8)
        data = res.json()

        addr = data.get("address", {})
        road = addr.get("road", "")
        house_number = addr.get("house_number", "")
        postcode = addr.get("postcode", "")
        city = addr.get("city") or addr.get("town") or addr.get("village") or ""
        country = addr.get("country", "")

        short_address = " ".join(x for x in [road, house_number] if x).strip()
        if city:
            if short_address:
                short_address += ", " + city
            else:
                short_address = city

        return {
            "display_name": data.get("display_name", "Unbekannt"),
            "road": road,
            "house_number": house_number,
            "postcode": postcode,
            "city": city,
            "country": country,
            "short_address": short_address or "Unbekannt"
        }

    except Exception:
        return {
            "display_name": "Unbekannt",
            "road": "",
            "house_number": "",
            "postcode": "",
            "city": "",
            "country": "",
            "short_address": "Unbekannt"
        }
    except Exception:
        return {
            "display_name": "Unbekannt",
            "road": "",
            "house_number": "",
            "postcode": "",
            "city": "",
            "country": "",
            "short_address": "Unbekannt"
        }
# ===== DATA =====
def get_points(device, start_ts=None, end_ts=None):
    conn = get_db()
    cur = conn.cursor()

    query = "SELECT lat, lon, recorded_at FROM location_points WHERE device=?"
    params = [device]

    if start_ts is not None:
        query += " AND recorded_at >= ?"
        params.append(start_ts)

    if end_ts is not None:
        query += " AND recorded_at <= ?"
        params.append(end_ts)

    query += " ORDER BY recorded_at"

    cur.execute(query, tuple(params))
    rows = cur.fetchall()
    conn.close()
    return rows

# ===== STAYS =====
def compute_stays(device, stay_radius_m=80, min_stay_sec=10, start_ts=None, end_ts=None):
    rows = get_points(device, start_ts, end_ts)

    if not rows:
        return []

    stays = []
    cluster = [rows[0]]

    for row in rows[1:]:
        prev = cluster[-1]
        dist = haversine_m(prev[0], prev[1], row[0], row[1])

        if dist <= stay_radius_m:
            cluster.append(row)
        else:
            start_ts_cluster = cluster[0][2]
            end_ts_cluster = cluster[-1][2]
            duration_sec = max(0, (end_ts_cluster - start_ts_cluster) // 1000)

            if duration_sec >= min_stay_sec:
                avg_lat = sum(p[0] for p in cluster) / len(cluster)
                avg_lon = sum(p[1] for p in cluster) / len(cluster)
                addr = get_address(avg_lat, avg_lon)

                stays.append({
                    "device": device,
                    "label": addr.get("short_address") or f"{avg_lat:.5f}, {avg_lon:.5f}",
                    "address": addr.get("display_name", "Unbekannt"),
                    "lat": round(avg_lat, 6),
                    "lon": round(avg_lon, 6),
                    "arrival": fmt_ts(start_ts_cluster),
                    "departure": fmt_ts(end_ts_cluster),
                    "start_ts": start_ts_cluster,
                    "end_ts": end_ts_cluster,
                    "duration_sec": duration_sec,
                    "duration_human": fmt_duration(duration_sec),
                    "point_count": len(cluster)
                })

            cluster = [row]

    start_ts_cluster = cluster[0][2]
    end_ts_cluster = cluster[-1][2]
    duration_sec = max(0, (end_ts_cluster - start_ts_cluster) // 1000)

    if duration_sec >= min_stay_sec:
        avg_lat = sum(p[0] for p in cluster) / len(cluster)
        avg_lon = sum(p[1] for p in cluster) / len(cluster)
        addr = get_address(avg_lat, avg_lon)

        stays.append({
            "device": device,
            "label": addr.get("short_address") or f"{avg_lat:.5f}, {avg_lon:.5f}",
            "address": addr.get("display_name", "Unbekannt"),
            "lat": round(avg_lat, 6),
            "lon": round(avg_lon, 6),
            "arrival": fmt_ts(start_ts_cluster),
            "departure": fmt_ts(end_ts_cluster),
            "start_ts": start_ts_cluster,
            "end_ts": end_ts_cluster,
            "duration_sec": duration_sec,
            "duration_human": fmt_duration(duration_sec),
            "point_count": len(cluster)
        })

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
    
@app.route("/points")
def api_points():
    device = request.args.get("device")

    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT lat, lon
        FROM location_points
        WHERE device = ?
        ORDER BY recorded_at DESC
        LIMIT 100
    """, (device,))
    rows = cur.fetchall()
    conn.close()

    return jsonify({
        "points": [{"lat": r[0], "lon": r[1]} for r in rows]
    })

@app.route("/reverse")
def api_reverse():
    lat = request.args.get("lat", type=float)
    lon = request.args.get("lon", type=float)

    if lat is None or lon is None:
        return jsonify({"error": "missing lat/lon"}), 400

    addr = get_address(lat, lon)
    return jsonify(addr)
    
@app.route("/track")
def api_track():
    device = request.args.get("device")

    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT lat, lon
        FROM location_points
        WHERE device = ?
        ORDER BY recorded_at ASC
        LIMIT 500
    """, (device,))
    rows = cur.fetchall()
    conn.close()

    return jsonify({
        "track": [{"lat": r[0], "lon": r[1]} for r in rows]
    })

def summarize_places(device, start_ts=None, end_ts=None):
    stays = compute_stays(device, start_ts=start_ts, end_ts=end_ts)
    places = {}

    for s in stays:
        key = s.get("address") or s.get("label")
        if key not in places:
            places[key] = {
                "label": key,
                "lat": s["lat"],
                "lon": s["lon"],
                "visit_count": 0,
                "total_duration_sec": 0,
                "last_arrival": s["arrival"],
                "last_departure": s["departure"]
            }

        places[key]["visit_count"] += 1
        places[key]["total_duration_sec"] += s["duration_sec"]
        places[key]["last_arrival"] = s["arrival"]
        places[key]["last_departure"] = s["departure"]

    result = []
    for v in places.values():
        v["total_duration_human"] = fmt_duration(v["total_duration_sec"])
        result.append(v)

    result.sort(key=lambda x: x["total_duration_sec"], reverse=True)

    if len(result) > 0:
        result[0]["kind"] = "Zuhause"
    if len(result) > 1:
        result[1]["kind"] = "Arbeit"
    for r in result[2:]:
        r["kind"] = "Ort"

    return result

def day_range(filter_name):
    now = int(time.time())
    lt = time.localtime(now)

    if filter_name == "today":
        start = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 0, 0, 0, lt.tm_wday, lt.tm_yday, lt.tm_isdst))
        end = start + 86400 - 1
        return int(start * 1000), int(end * 1000)

    if filter_name == "yesterday":
        today_start = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 0, 0, 0, lt.tm_wday, lt.tm_yday, lt.tm_isdst))
        start = today_start - 86400
        end = today_start - 1
        return int(start * 1000), int(end * 1000)

    return None, None

def send_email_alert(subject, body):
    api_key = os.environ.get("RESEND_API_KEY")
    if not api_key:
        return {"ok": False, "error": "RESEND_API_KEY missing"}

    payload = {
        "from": "onboarding@resend.dev",
        "to": ["benhurhoher@gmail.com"],
        "subject": subject,
        "text": body
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    try:
        r = requests.post(
            "https://api.resend.com/emails",
            json=payload,
            headers=headers,
            timeout=15
        )
        return {"ok": r.ok, "status_code": r.status_code, "response": r.text}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.route("/test-email")
def test_email():
    result = send_email_alert(
        "Panda2 Test Email",
        "Das ist die erste Email-Benachrichtigung vom Panda2 Tracker."
    )
    return jsonify(result)
    
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
            html, body {
                margin: 0;
                padding: 0;
                height: 100%;
            }
            #map {
                height: 100vh;
                width: 100%;
            }
        </style>
    </head>
    <body>
        <div id="map"></div>

        <script src="https://unpkg.com/leaflet/dist/leaflet.js"></script>
        <script>
            var device = "__DEVICE__";
            var map = L.map("map").setView([51, 10], 6);

            L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
                maxZoom: 19,
                attribution: "&copy; OpenStreetMap"
            }).addTo(map);

            var markers = L.layerGroup().addTo(map);
            var routeLine = null;
            window._mapInitialized = false;

            function loadLiveMap() {
                markers.clearLayers();

                fetch("/points?device=" + encodeURIComponent(device))
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    var pts = data.points || [];
                    var bounds = [];

                    pts.forEach(function(p) {
                        var marker = L.marker([p.lat, p.lon]).addTo(markers);

                        marker.on("click", function() {
                            fetch("/reverse?lat=" + p.lat + "&lon=" + p.lon)
                            .then(function(r) { return r.json(); })
                            .then(function(addr) {
                                marker.bindPopup(
                                    "<b>" + (addr.short_address || "Unbekannt") + "</b><br>" +
                                    "Straße: " + (addr.road || "-") + "<br>" +
                                    "Hausnr: " + (addr.house_number || "-") + "<br>" +
                                    "Ort: " + (addr.city || "-")
                                ).openPopup();
                            });
                        });

                        bounds.push([p.lat, p.lon]);
                    });

                    fetch("/track?device=" + encodeURIComponent(device))
                    .then(function(r) { return r.json(); })
                    .then(function(trackData) {
                        var coords = (trackData.track || []).map(p => [p.lat, p.lon]);

                        if (routeLine) {
                            map.removeLayer(routeLine);
                        }

                        if (coords.length > 1) {
                            routeLine = L.polyline(coords, {
                                color: "red",
                                weight: 4
                            }).addTo(map);
                        }

                        if (bounds.length > 0 && !window._mapInitialized) {
                            map.fitBounds(bounds);
                            window._mapInitialized = true;
                        }
                   });
              });
           }

           // einmal starten
           loadLiveMap();

           // alle 5 Sekunden aktualisieren
           setInterval(loadLiveMap, 5000);

        </script>
    </body>
    </html>
    """

    html = html.replace("__DEVICE__", device)
    return Response(html, mimetype="text/html")

# run via gunicorn
