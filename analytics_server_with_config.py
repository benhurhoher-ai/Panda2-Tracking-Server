from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3
import time

app = Flask(__name__)
CORS(app)

DB_PATH = "locations.db"

# -------------------------
# DATABASE
# -------------------------
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

# -------------------------
# CONFIG LOGIC
# -------------------------
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

# -------------------------
# ROUTES
# -------------------------

@app.route("/")
def home():
    return jsonify({
        "name": "Location Analytics Backend",
        "status": "ok",
        "endpoints": [
            "POST /location",
            "GET /config/<device>"
        ]
    })

@app.route("/config/<device>", methods=["GET"])
def api_get_config(device):
    return jsonify(get_device_config(device))

@app.route("/location", methods=["POST"])
def api_location():
    data = request.json

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
    """, (device, lat, lon, accuracy, int(time.time() * 1000)))

    conn.commit()
    conn.close()

    return jsonify({"status": "ok"})

# -------------------------
# START
# -------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
