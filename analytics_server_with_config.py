
from __future__ import annotations

import math
import sqlite3
from collections import defaultdict
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from flask import Flask, jsonify, request
from flask_cors import CORS

DB_PATH = "locations.db"

MAX_ALLOWED_ACCURACY_M = 100.0
MAX_ALLOWED_SPEED_M_S = 60.0
STAY_DISTANCE_THRESHOLD_M = 80.0
STAY_TIME_THRESHOLD_SEC = 8 * 60
PLACE_MERGE_RADIUS_M = 120.0
MIN_TRIP_DISTANCE_M = 150.0
MIN_TRIP_DURATION_SEC = 60

app = Flask(__name__)
CORS(app)

@dataclass
class LocationPoint:
    device: str
    lat: float
    lon: float
    recorded_at: int
    accuracy: float | None = None
    speed: float | None = None
    altitude: float | None = None
    heading: float | None = None

@dataclass
class StayPoint:
    device: str
    start_ts: int
    end_ts: int
    center_lat: float
    center_lon: float
    point_count: int
    duration_sec: int

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    with closing(get_db()) as conn:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS location_points (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            accuracy REAL,
            speed REAL,
            altitude REAL,
            heading REAL,
            recorded_at INTEGER NOT NULL
        )
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_location_device_time
        ON location_points(device, recorded_at)
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS device_config (
            device TEXT PRIMARY KEY,
            tracking_enabled INTEGER NOT NULL DEFAULT 1,
            interval_sec INTEGER NOT NULL DEFAULT 30,
            updated_at INTEGER NOT NULL
        )
        """)
        conn.commit()

def now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)

def row_to_point(row: sqlite3.Row) -> LocationPoint:
    return LocationPoint(
        device=row["device"],
        lat=row["lat"],
        lon=row["lon"],
        recorded_at=row["recorded_at"],
        accuracy=row["accuracy"],
        speed=row["speed"],
        altitude=row["altitude"],
        heading=row["heading"],
    )

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def mean_coord(points: list[LocationPoint]) -> tuple[float, float]:
    lat = sum(p.lat for p in points) / len(points)
    lon = sum(p.lon for p in points) / len(points)
    return lat, lon

def ts_to_hour_utc(ts_ms: int) -> int:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).hour

def ts_to_weekday_utc(ts_ms: int) -> int:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).weekday()

def ts_to_day_utc(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

def point_to_dict(p: LocationPoint) -> dict[str, Any]:
    return {
        "device": p.device,
        "lat": p.lat,
        "lon": p.lon,
        "accuracy": p.accuracy,
        "speed": p.speed,
        "altitude": p.altitude,
        "heading": p.heading,
        "recorded_at": p.recorded_at,
    }

def stay_to_dict(s: StayPoint) -> dict[str, Any]:
    return {
        "device": s.device,
        "start_ts": s.start_ts,
        "end_ts": s.end_ts,
        "center_lat": s.center_lat,
        "center_lon": s.center_lon,
        "point_count": s.point_count,
        "duration_sec": s.duration_sec,
    }

def format_duration(seconds: int) -> str:
    h = seconds // 3600
    m = (seconds % 3600) // 60
    if h > 0:
        return f"{h}h {m}min"
    return f"{m}min"

def insert_location(data: dict[str, Any]) -> None:
    required = ["device", "lat", "lon", "timestamp"]
    for key in required:
        if key not in data:
            raise ValueError(f"Missing required field: {key}")
    ensure_device_config(str(data["device"]))
    with closing(get_db()) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO location_points
            (device, lat, lon, accuracy, speed, altitude, heading, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            str(data["device"]),
            float(data["lat"]),
            float(data["lon"]),
            float(data.get("accuracy")) if data.get("accuracy") is not None else None,
            float(data.get("speed")) if data.get("speed") is not None else None,
            float(data.get("altitude")) if data.get("altitude") is not None else None,
            float(data.get("heading")) if data.get("heading") is not None else None,
            int(data["timestamp"]),
        ))
        conn.commit()

def fetch_points(device: str, start_ts: int | None = None, end_ts: int | None = None) -> list[LocationPoint]:
    query = """
        SELECT device, lat, lon, accuracy, speed, altitude, heading, recorded_at
        FROM location_points
        WHERE device = ?
    """
    params: list[Any] = [device]
    if start_ts is not None:
        query += " AND recorded_at >= ?"
        params.append(start_ts)
    if end_ts is not None:
        query += " AND recorded_at <= ?"
        params.append(end_ts)
    query += " ORDER BY recorded_at ASC"
    with closing(get_db()) as conn:
        rows = conn.execute(query, params).fetchall()
    return [row_to_point(r) for r in rows]

def filter_points(points: list[LocationPoint]) -> list[LocationPoint]:
    if not points:
        return []
    filtered = [p for p in points if p.accuracy is None or p.accuracy <= MAX_ALLOWED_ACCURACY_M]
    if not filtered:
        return []
    cleaned = [filtered[0]]
    for p in filtered[1:]:
        prev = cleaned[-1]
        dt_sec = (p.recorded_at - prev.recorded_at) / 1000
        if dt_sec <= 0:
            continue
        dist = haversine_m(prev.lat, prev.lon, p.lat, p.lon)
        inferred_speed = dist / dt_sec
        candidate_speed = inferred_speed
        if p.speed is not None and p.speed > 0:
            candidate_speed = min(candidate_speed, p.speed)
        if candidate_speed <= MAX_ALLOWED_SPEED_M_S:
            cleaned.append(p)
    return cleaned

def detect_stays(points: list[LocationPoint], distance_threshold_m: float = STAY_DISTANCE_THRESHOLD_M,
                 time_threshold_sec: int = STAY_TIME_THRESHOLD_SEC) -> list[StayPoint]:
    if not points:
        return []
    stays: list[StayPoint] = []
    i = 0
    while i < len(points):
        j = i + 1
        cluster = [points[i]]
        while j < len(points):
            center_lat, center_lon = mean_coord(cluster)
            d = haversine_m(center_lat, center_lon, points[j].lat, points[j].lon)
            if d <= distance_threshold_m:
                cluster.append(points[j])
                j += 1
            else:
                break
        start_ts = cluster[0].recorded_at
        end_ts = cluster[-1].recorded_at
        duration_sec = max(0, (end_ts - start_ts) // 1000)
        if duration_sec >= time_threshold_sec:
            center_lat, center_lon = mean_coord(cluster)
            stays.append(StayPoint(
                device=cluster[0].device,
                start_ts=start_ts,
                end_ts=end_ts,
                center_lat=center_lat,
                center_lon=center_lon,
                point_count=len(cluster),
                duration_sec=duration_sec,
            ))
        i = max(j, i + 1)
    return stays

def cluster_places(stays: list[StayPoint]) -> list[dict[str, Any]]:
    places: list[dict[str, Any]] = []
    for stay in stays:
        matched_place = None
        for place in places:
            d = haversine_m(stay.center_lat, stay.center_lon, place["center_lat"], place["center_lon"])
            if d <= PLACE_MERGE_RADIUS_M:
                matched_place = place
                break
        visit = stay_to_dict(stay)
        if matched_place is None:
            places.append({
                "place_id": len(places) + 1,
                "device": stay.device,
                "label": None,
                "center_lat": stay.center_lat,
                "center_lon": stay.center_lon,
                "visit_count": 1,
                "total_duration_sec": stay.duration_sec,
                "is_home": False,
                "is_work": False,
                "visits": [visit],
            })
        else:
            matched_place["visits"].append(visit)
            matched_place["visit_count"] += 1
            matched_place["total_duration_sec"] += stay.duration_sec
            n = len(matched_place["visits"])
            matched_place["center_lat"] = (
                matched_place["center_lat"] * (n - 1) + stay.center_lat
            ) / n
            matched_place["center_lon"] = (
                matched_place["center_lon"] * (n - 1) + stay.center_lon
            ) / n
    return places

def score_place(place: dict[str, Any]) -> dict[str, int]:
    night_sec = 0
    work_sec = 0
    weekend_sec = 0
    for visit in place["visits"]:
        start_h = ts_to_hour_utc(visit["start_ts"])
        weekday = ts_to_weekday_utc(visit["start_ts"])
        dur = int(visit["duration_sec"])
        if start_h >= 21 or start_h <= 6:
            night_sec += dur
        if weekday <= 4 and 8 <= start_h <= 18:
            work_sec += dur
        if weekday >= 5:
            weekend_sec += dur
    return {
        "night_sec": night_sec,
        "work_sec": work_sec,
        "weekend_sec": weekend_sec,
        "total_sec": int(place["total_duration_sec"]),
        "visits": int(place["visit_count"]),
    }

def label_places(places: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not places:
        return places
    scored = [(place, score_place(place)) for place in places]
    home_candidate = max(scored, key=lambda x: (x[1]["night_sec"], x[1]["visits"], x[1]["weekend_sec"]), default=None)
    if home_candidate and home_candidate[1]["night_sec"] > 0:
        home_candidate[0]["label"] = "Zuhause"
        home_candidate[0]["is_home"] = True
    work_candidates = [item for item in scored if not item[0].get("is_home")]
    if work_candidates:
        work_candidate = max(work_candidates, key=lambda x: (x[1]["work_sec"], x[1]["visits"]), default=None)
        if work_candidate and work_candidate[1]["work_sec"] > 0:
            work_candidate[0]["label"] = "Arbeit"
            work_candidate[0]["is_work"] = True
    unnamed_counter = 1
    for place, metrics in scored:
        if not place["label"]:
            if metrics["visits"] >= 3 and metrics["total_sec"] >= 2 * 3600:
                place["label"] = f"Wiederkehrender Ort {unnamed_counter}"
                unnamed_counter += 1
            else:
                place["label"] = "Unbekannter Ort"
        place["score"] = metrics
    return places

def match_place_for_stay(stay: StayPoint, places: list[dict[str, Any]]) -> int | None:
    best_place_id = None
    best_distance = None
    for place in places:
        d = haversine_m(stay.center_lat, stay.center_lon, place["center_lat"], place["center_lon"])
        if d <= PLACE_MERGE_RADIUS_M:
            if best_distance is None or d < best_distance:
                best_distance = d
                best_place_id = place["place_id"]
    return best_place_id

def detect_trips(stays: list[StayPoint], places: list[dict[str, Any]]) -> list[dict[str, Any]]:
    trips: list[dict[str, Any]] = []
    for i in range(len(stays) - 1):
        a = stays[i]
        b = stays[i + 1]
        duration_sec = max(0, (b.start_ts - a.end_ts) // 1000)
        distance_m = haversine_m(a.center_lat, a.center_lon, b.center_lat, b.center_lon)
        if duration_sec < MIN_TRIP_DURATION_SEC or distance_m < MIN_TRIP_DISTANCE_M:
            continue
        start_place_id = match_place_for_stay(a, places)
        end_place_id = match_place_for_stay(b, places)
        start_label = next((p["label"] for p in places if p["place_id"] == start_place_id), None)
        end_label = next((p["label"] for p in places if p["place_id"] == end_place_id), None)
        trips.append({
            "start_ts": a.end_ts,
            "end_ts": b.start_ts,
            "duration_sec": duration_sec,
            "distance_m": round(distance_m, 1),
            "start_place_id": start_place_id,
            "end_place_id": end_place_id,
            "start_label": start_label,
            "end_label": end_label,
        })
    return trips

def summarize_days(places: list[dict[str, Any]], trips: list[dict[str, Any]]) -> list[dict[str, Any]]:
    day_summary: dict[str, dict[str, Any]] = {}
    for place in places:
        for visit in place["visits"]:
            day = ts_to_day_utc(visit["start_ts"])
            if day not in day_summary:
                day_summary[day] = {"date": day, "durations_sec": defaultdict(int), "durations_human": {}, "trip_count": 0}
            day_summary[day]["durations_sec"][place["label"]] += int(visit["duration_sec"])
    for trip in trips:
        day = ts_to_day_utc(trip["start_ts"])
        if day not in day_summary:
            day_summary[day] = {"date": day, "durations_sec": defaultdict(int), "durations_human": {}, "trip_count": 0}
        day_summary[day]["trip_count"] += 1
    result = []
    for _, entry in sorted(day_summary.items()):
        entry["durations_sec"] = dict(entry["durations_sec"])
        entry["durations_human"] = {label: format_duration(sec) for label, sec in entry["durations_sec"].items()}
        result.append(entry)
    return result

def compute_analytics(device: str, start_ts: int | None = None, end_ts: int | None = None) -> dict[str, Any]:
    raw_points = fetch_points(device, start_ts, end_ts)
    filtered_points = filter_points(raw_points)
    stays = detect_stays(filtered_points)
    places = label_places(cluster_places(stays))
    trips = detect_trips(stays, places)
    day_summary = summarize_days(places, trips)
    return {
        "device": device,
        "point_count_raw": len(raw_points),
        "point_count_filtered": len(filtered_points),
        "stays": [stay_to_dict(s) for s in stays],
        "places": [{
            "place_id": p["place_id"],
            "device": p["device"],
            "label": p["label"],
            "center_lat": p["center_lat"],
            "center_lon": p["center_lon"],
            "visit_count": p["visit_count"],
            "total_duration_sec": p["total_duration_sec"],
            "total_duration_human": format_duration(int(p["total_duration_sec"])),
            "is_home": p["is_home"],
            "is_work": p["is_work"],
            "score": p.get("score", {}),
            "visits": p["visits"],
        } for p in places],
        "trips": trips,
        "days": day_summary,
    }

def ensure_device_config(device: str) -> None:
    with closing(get_db()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT device FROM device_config WHERE device = ?", (device,))
        row = cur.fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO device_config (device, tracking_enabled, interval_sec, updated_at) VALUES (?, ?, ?, ?)",
                (device, 1, 30, now_ms()),
            )
            conn.commit()

def get_device_config(device: str) -> dict[str, Any]:
    ensure_device_config(device)
    with closing(get_db()) as conn:
        row = conn.execute(
            "SELECT device, tracking_enabled, interval_sec, updated_at FROM device_config WHERE device = ?",
            (device,),
        ).fetchone()
    return {
        "device": row["device"],
        "tracking_enabled": bool(row["tracking_enabled"]),
        "interval_sec": int(row["interval_sec"]),
        "updated_at": int(row["updated_at"]),
    }

def update_device_config(device: str, tracking_enabled: bool | None = None, interval_sec: int | None = None) -> dict[str, Any]:
    ensure_device_config(device)
    current = get_device_config(device)
    enabled_val = int(current["tracking_enabled"] if tracking_enabled is None else tracking_enabled)
    interval_val = current["interval_sec"] if interval_sec is None else max(10, min(int(interval_sec), 3600))
    with closing(get_db()) as conn:
        conn.execute(
            "UPDATE device_config SET tracking_enabled = ?, interval_sec = ?, updated_at = ? WHERE device = ?",
            (enabled_val, interval_val, now_ms(), device),
        )
        conn.commit()
    return get_device_config(device)

@app.get("/")
def root():
    return jsonify({
        "name": "Location Analytics Backend",
        "status": "ok",
        "endpoints": [
            "POST /location",
            "GET /devices",
            "GET /points?device=tracker_1",
            "GET /analytics?device=tracker_1",
            "GET /analytics/places?device=tracker_1",
            "GET /analytics/stays?device=tracker_1",
            "GET /analytics/trips?device=tracker_1",
            "GET /analytics/day-summary?device=tracker_1",
            "GET /config/<deviceId>",
            "POST /config/<deviceId>",
        ],
    })

@app.post("/location")
def post_location():
    try:
        data = request.get_json(force=True)
        insert_location(data)
        return jsonify({"status": "ok"}), 201
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

@app.get("/devices")
def get_devices():
    with closing(get_db()) as conn:
        rows = conn.execute("""
            SELECT lp.device, COUNT(*) AS point_count,
                   MIN(lp.recorded_at) AS first_seen,
                   MAX(lp.recorded_at) AS last_seen,
                   COALESCE(dc.tracking_enabled, 1) AS tracking_enabled,
                   COALESCE(dc.interval_sec, 30) AS interval_sec
            FROM location_points lp
            LEFT JOIN device_config dc ON dc.device = lp.device
            GROUP BY lp.device
            ORDER BY lp.device ASC
        """).fetchall()
        config_only = conn.execute("""
            SELECT dc.device, 0 AS point_count, NULL AS first_seen, NULL AS last_seen,
                   dc.tracking_enabled, dc.interval_sec
            FROM device_config dc
            WHERE dc.device NOT IN (SELECT DISTINCT device FROM location_points)
            ORDER BY dc.device ASC
        """).fetchall()
    all_rows = list(rows) + list(config_only)
    return jsonify({
        "devices": [{
            "device": r["device"],
            "point_count": r["point_count"],
            "first_seen": r["first_seen"],
            "last_seen": r["last_seen"],
            "tracking_enabled": bool(r["tracking_enabled"]),
            "interval_sec": int(r["interval_sec"]),
        } for r in all_rows]
    })

@app.get("/points")
def get_points():
    device = request.args.get("device")
    if not device:
        return jsonify({"status": "error", "message": "Missing query param: device"}), 400
    start_ts = request.args.get("start_ts", type=int)
    end_ts = request.args.get("end_ts", type=int)
    points = fetch_points(device, start_ts, end_ts)
    return jsonify({"device": device, "count": len(points), "points": [point_to_dict(p) for p in points]})

@app.get("/analytics")
def get_analytics():
    device = request.args.get("device")
    if not device:
        return jsonify({"status": "error", "message": "Missing query param: device"}), 400
    start_ts = request.args.get("start_ts", type=int)
    end_ts = request.args.get("end_ts", type=int)
    return jsonify(compute_analytics(device, start_ts, end_ts))

@app.get("/analytics/stays")
def get_stays():
    device = request.args.get("device")
    if not device:
        return jsonify({"status": "error", "message": "Missing query param: device"}), 400
    start_ts = request.args.get("start_ts", type=int)
    end_ts = request.args.get("end_ts", type=int)
    analytics = compute_analytics(device, start_ts, end_ts)
    return jsonify({"device": device, "count": len(analytics["stays"]), "stays": analytics["stays"]})

@app.get("/analytics/places")
def get_places():
    device = request.args.get("device")
    if not device:
        return jsonify({"status": "error", "message": "Missing query param: device"}), 400
    start_ts = request.args.get("start_ts", type=int)
    end_ts = request.args.get("end_ts", type=int)
    analytics = compute_analytics(device, start_ts, end_ts)
    return jsonify({"device": device, "count": len(analytics["places"]), "places": analytics["places"]})

@app.get("/analytics/trips")
def get_trips():
    device = request.args.get("device")
    if not device:
        return jsonify({"status": "error", "message": "Missing query param: device"}), 400
    start_ts = request.args.get("start_ts", type=int)
    end_ts = request.args.get("end_ts", type=int)
    analytics = compute_analytics(device, start_ts, end_ts)
    return jsonify({"device": device, "count": len(analytics["trips"]), "trips": analytics["trips"]})

@app.get("/analytics/day-summary")
def get_day_summary():
    device = request.args.get("device")
    if not device:
        return jsonify({"status": "error", "message": "Missing query param: device"}), 400
    start_ts = request.args.get("start_ts", type=int)
    end_ts = request.args.get("end_ts", type=int)
    analytics = compute_analytics(device, start_ts, end_ts)
    return jsonify({"device": device, "days": analytics["days"]})

@app.get("/config/<device_id>")
def api_get_config(device_id: str):
    return jsonify(get_device_config(device_id))

@app.post("/config/<device_id>")
def api_post_config(device_id: str):
    data = request.get_json(force=True, silent=True) or {}
    tracking_enabled = data.get("tracking_enabled")
    interval_sec = data.get("interval_sec")
    if tracking_enabled is not None and not isinstance(tracking_enabled, bool):
        return jsonify({"status": "error", "message": "tracking_enabled must be boolean"}), 400
    if interval_sec is not None:
        try:
            interval_sec = int(interval_sec)
        except Exception:
            return jsonify({"status": "error", "message": "interval_sec must be integer"}), 400
    updated = update_device_config(device_id, tracking_enabled=tracking_enabled, interval_sec=interval_sec)
    return jsonify({"status": "ok", "config": updated})

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=True)
