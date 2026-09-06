#!/usr/bin/env python3
"""
F1 Mock API - OpenF1-shaped mock for testing and simulation.

Exposes the upstream OpenF1 endpoints the predictor consumes:
  GET /health
  GET /v1/meetings
  GET /v1/sessions
  GET /v1/drivers
  GET /v1/starting_grid
  GET /v1/session_result
  GET /v1/race_control

Also keeps the legacy Ergast-compatible routes for backwards compatibility:
  GET /<season>.json
  GET /<season>/drivers.json
  GET /<season>/<round>/results.json

Seeds from Ergast API on startup if empty, so races/drivers start populated.
Admin UI/endpoints control race state: start times, finish, podium.
"""

import json
import os
import sqlite3
import requests
from datetime import datetime, timezone
from urllib.parse import urljoin

from flask import Flask, g, jsonify, render_template, request, redirect, url_for, flash

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
app.config['DATABASE'] = os.environ.get('DATABASE_PATH', '/data/f1_mock.db')
app.config['ERGAST_BASE'] = os.environ.get('ERGAST_BASE', 'https://api.jolpi.ca/ergast/f1/')
app.config['DEFAULT_SEASON'] = int(os.environ.get('DEFAULT_SEASON', '2024'))

# Placeholder constructor for admin-set podium results (no constructor in drivers API)
PLACEHOLDER_CONSTRUCTOR = {
    "constructorId": "mock",
    "url": "https://en.wikipedia.org/wiki/Formula_1",
    "name": "Mock",
    "nationality": "",
}

# OpenF1-style status values for session_result rows
_STATUS_FINISHED = "Finished"
_STATUS_DNF = "+1 Lap"


def get_db():
    """Get database connection for current request."""
    if 'db' not in g:
        g.db = sqlite3.connect(app.config['DATABASE'])
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(e=None):
    """Close database connection at end of request."""
    db = g.pop('db', None)
    if db is not None:
        db.close()


def init_db():
    """Create database schema."""
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS seasons (
            season TEXT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS races (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season TEXT NOT NULL,
            round TEXT NOT NULL,
            race_name TEXT,
            circuit_id TEXT,
            circuit_name TEXT,
            circuit_url TEXT,
            locality TEXT,
            country TEXT,
            lat TEXT,
            long TEXT,
            race_url TEXT,
            date TEXT,
            time TEXT,
            start_override TEXT,
            has_results INTEGER DEFAULT 0,
            p1_driver_id TEXT,
            p2_driver_id TEXT,
            p3_driver_id TEXT,
            raw_json TEXT,
            UNIQUE(season, round),
            FOREIGN KEY (season) REFERENCES seasons(season)
        );

        CREATE TABLE IF NOT EXISTS drivers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season TEXT NOT NULL,
            driver_id TEXT NOT NULL,
            permanent_number TEXT,
            code TEXT,
            url TEXT,
            given_name TEXT,
            family_name TEXT,
            date_of_birth TEXT,
            nationality TEXT,
            raw_json TEXT,
            UNIQUE(season, driver_id),
            FOREIGN KEY (season) REFERENCES seasons(season)
        );

        CREATE INDEX IF NOT EXISTS idx_races_season ON races(season);
        CREATE INDEX IF NOT EXISTS idx_drivers_season ON drivers(season);
    """)
    db.commit()


def _fetch_ergast(path: str) -> dict | None:
    """Fetch JSON from Ergast API."""
    url = urljoin(app.config['ERGAST_BASE'], path.lstrip('/'))
    if not path.endswith('.json'):
        url = url.rstrip('/') + '.json'
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        app.logger.warning("Ergast fetch failed %s: %s", url, e)
        return None


def _is_empty() -> bool:
    """Check if database has no races."""
    db = get_db()
    cur = db.execute("SELECT COUNT(*) FROM races")
    return cur.fetchone()[0] == 0


def _seed_season(season: str) -> bool:
    """Seed one season from Ergast API. Returns True on success."""
    # Races
    data = _fetch_ergast(f"/{season}.json")
    if not data:
        return False

    mrd = data.get("MRData", {})
    rt = mrd.get("RaceTable", {})
    races = rt.get("Races", [])

    db = get_db()
    db.execute("INSERT OR IGNORE INTO seasons (season) VALUES (?)", (season,))

    for race in races:
        circuit = race.get("Circuit", {}) or {}
        loc = circuit.get("Location", {}) or {}
        raw = json.dumps(race)
        db.execute("""
            INSERT OR REPLACE INTO races (
                season, round, race_name, circuit_id, circuit_name, circuit_url,
                locality, country, lat, long, race_url, date, time, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            race.get("season", season),
            race.get("round", ""),
            race.get("raceName", ""),
            circuit.get("circuitId", ""),
            circuit.get("circuitName", ""),
            circuit.get("url", ""),
            loc.get("locality", ""),
            loc.get("country", ""),
            loc.get("lat", ""),
            loc.get("long", ""),
            race.get("url", ""),
            race.get("date", ""),
            race.get("time", ""),
            raw,
        ))

    # Drivers
    drv_data = _fetch_ergast(f"/{season}/drivers.json")
    if drv_data:
        dt = drv_data.get("MRData", {}).get("DriverTable", {})
        for d in dt.get("Drivers", []):
            raw = json.dumps(d)
            db.execute("""
                INSERT OR REPLACE INTO drivers (
                    season, driver_id, permanent_number, code, url,
                    given_name, family_name, date_of_birth, nationality, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                season,
                d.get("driverId", ""),
                d.get("permanentNumber", ""),
                d.get("code", ""),
                d.get("url", ""),
                d.get("givenName", ""),
                d.get("familyName", ""),
                d.get("dateOfBirth", ""),
                d.get("nationality", ""),
                raw,
            ))

    db.commit()
    return True


def seed_if_empty():
    """Seed from Ergast if database is empty."""
    if _is_empty():
        season = str(app.config['DEFAULT_SEASON'])
        if _seed_season(season):
            app.logger.info("Seeded season %s from Ergast", season)
        else:
            app.logger.warning("Seed failed for season %s", season)


# ---------------------------------------------------------------------------
# OpenF1 helpers
# ---------------------------------------------------------------------------

def _season_or_default() -> str:
    return request.args.get("year", request.args.get("season", str(app.config['DEFAULT_SEASON'])))


def _race_to_openf1_session(race: dict) -> dict:
    """Convert a DB race row into an OpenF1 session record."""
    r = dict(race)
    # Prefer explicit start_override if present, else original date/time.
    date = r.get("date") or ""
    time_part = r.get("time") or ""
    if r.get("start_override"):
        try:
            dt = datetime.fromisoformat(r["start_override"].replace("Z", "+00:00"))
            date = dt.strftime("%Y-%m-%d")
            time_part = dt.strftime("%H:%M:%SZ")
        except Exception:
            pass

    date_start = f"{date}T{time_part}" if time_part else date
    round_no = int(r.get("round") or 0)
    return {
        "session_key": round_no,
        "session_name": r.get("race_name", "Race"),
        "session_type": "Race",
        "date_start": date_start,
        "year": int(r.get("season") or _season_or_default()),
        "meeting_key": round_no,
        "circuit_short_name": r.get("circuit_name", ""),
        "country_name": r.get("country", ""),
        "location": r.get("locality", ""),
    }


def _driver_to_openf1(drv: dict) -> dict:
    """Convert a DB driver row into an OpenF1 driver record."""
    given = drv.get("given_name") or ""
    family = drv.get("family_name") or ""
    number = drv.get("permanent_number") or drv.get("code")
    try:
        driver_number = int(number) if number else None
    except (ValueError, TypeError):
        driver_number = None
    return {
        "driver_number": driver_number,
        "full_name": f"{given} {family}".strip() or drv.get("driver_id", ""),
        "first_name": given,
        "last_name": family,
        "name_acronym": drv.get("code", ""),
        "driver_id": drv.get("driver_id", ""),
        "team_name": "",
        "country_code": _country_to_code(drv.get("nationality", "")),
    }


def _country_to_code(nationality: str) -> str:
    """Best-effort nationality -> ISO country code for OpenF1 compatibility."""
    mapping = {
        "Dutch": "NL",
        "British": "GB",
        "Monegasque": "MC",
        "Australian": "AU",
        "German": "DE",
        "French": "FR",
        "Spanish": "ES",
        "Italian": "IT",
        "Canadian": "CA",
        "Mexican": "MX",
        "Finnish": "FI",
        "Dane": "DK",
        "Swiss": "CH",
        "Thai": "TH",
        "Japanese": "JP",
        "Chinese": "CN",
        "American": "US",
        "Argentine": "AR",
        "Brazilian": "BR",
        "Austrian": "AT",
        "Belgian": "BE",
        "Hungarian": "HU",
        "Polish": "PL",
        "New Zealander": "NZ",
        "Swedish": "SE",
    }
    return mapping.get(nationality, "")


def _result_from_podium(drv: dict, position: int) -> dict:
    """Build an OpenF1 session_result row from a driver and podium position."""
    number = drv.get("permanent_number") or str(position)
    try:
        driver_number = int(number)
    except (ValueError, TypeError):
        driver_number = int(drv.get("code", "0")) or position
    points_map = {1: 26, 2: 18, 3: 15}
    return {
        "position": position,
        "driver_number": driver_number,
        "points": points_map.get(position, 0),
        "status": "Finished",
        "dnf": False,
        "dns": False,
        "dsq": False,
    }


def _get_session_results(season: str, round_no: str):
    """Return OpenF1-shaped session_result rows if the race is finished."""
    db = get_db()
    race = db.execute(
        "SELECT * FROM races WHERE season = ? AND round = ?",
        (season, round_no),
    ).fetchone()
    if not race or not race["has_results"]:
        return []

    podium_ids = [race["p1_driver_id"], race["p2_driver_id"], race["p3_driver_id"]]
    if not any(podium_ids):
        return []

    drivers = {}
    for did in podium_ids:
        if did:
            row = db.execute(
                "SELECT * FROM drivers WHERE season = ? AND driver_id = ?",
                (season, did),
            ).fetchone()
            if row:
                drivers[did] = dict(row)

    results = []
    for pos, did in enumerate(podium_ids, 1):
        if not did or did not in drivers:
            continue
        results.append(_result_from_podium(drivers[did], pos))
    return results


def _get_openf1_race_by_round(season: str, round_no: str) -> dict | None:
    """Return an OpenF1 session/meeting-style record for a single round."""
    db = get_db()
    row = db.execute(
        "SELECT * FROM races WHERE season = ? AND round = ?",
        (season, round_no),
    ).fetchone()
    return _race_to_openf1_session(dict(row)) if row else None


def _get_openf1_races(season: str) -> list[dict]:
    """Return OpenF1-shaped sessions for all races in a season."""
    db = get_db()
    rows = db.execute(
        "SELECT * FROM races WHERE season = ? ORDER BY round",
        (season,),
    ).fetchall()
    return [_race_to_openf1_session(dict(r)) for r in rows]


def _get_openf1_drivers(season: str) -> list[dict]:
    """Return OpenF1-shaped drivers for a season."""
    db = get_db()
    rows = db.execute(
        "SELECT * FROM drivers WHERE season = ? ORDER BY family_name",
        (season,),
    ).fetchall()
    return [_driver_to_openf1(dict(r)) for r in rows]


def _get_round_from_session_key(session_key: str) -> str:
    """Map a session_key query param to a round string (the predictor expects round==key)."""
    return str(session_key)


# ---------------------------------------------------------------------------
# OpenF1 routes
# ---------------------------------------------------------------------------

@app.route("/v1/sessions")
def openf1_sessions():
    """GET /v1/sessions?year=<season>&session_type=Race"""
    session_type = request.args.get("session_type")
    season = _season_or_default()
    sessions = _get_openf1_races(season)
    if session_type:
        sessions = [s for s in sessions if s.get("session_type") == session_type]
    return jsonify(sessions)


@app.route("/v1/meetings")
def openf1_meetings():
    """GET /v1/meetings?year=<season>"""
    season = _season_or_default()
    meetings = []
    for race in _get_openf1_races(season):
        meetings.append({
            "meeting_key": race["meeting_key"],
            "meeting_name": race["session_name"],
            "meeting_official_name": race["session_name"],
            "date_start": race["date_start"],
            "year": race["year"],
            "circuit_short_name": race["circuit_short_name"],
            "country_name": race["country_name"],
            "location": race["location"],
        })
    return jsonify(meetings)


@app.route("/v1/drivers")
def openf1_drivers():
    """GET /v1/drivers?session_key=<key> or ?year=<season>"""
    session_key = request.args.get("session_key")
    season = request.args.get("year", request.args.get("season"))
    if session_key is not None:
        # session_key maps 1:1 to round, but drivers are stored per-season;
        # resolve season from the session.
        round_no = _get_round_from_session_key(session_key)
        db = get_db()
        race = db.execute(
            "SELECT season FROM races WHERE round = ?",
            (round_no,),
        ).fetchone()
        season = race["season"] if race else (season or _season_or_default())
    elif not season:
        season = _season_or_default()
    return jsonify(_get_openf1_drivers(season))


@app.route("/v1/starting_grid")
def openf1_starting_grid():
    """GET /v1/starting_grid?session_key=<key>

    The mock has no qualifying data; return an empty list.
    """
    return jsonify([])


@app.route("/v1/session_result")
def openf1_session_result():
    """GET /v1/session_result?session_key=<key>"""
    session_key = request.args.get("session_key")
    if session_key is None:
        return jsonify([])
    round_no = _get_round_from_session_key(session_key)
    season = _season_or_default()
    return jsonify(_get_session_results(season, round_no))


@app.route("/v1/race_control")
def openf1_race_control():
    """GET /v1/race_control?session_key=<key>

    The mock has no race-control feed; return an empty list.
    """
    return jsonify([])


# ---------------------------------------------------------------------------
# Legacy Ergast routes (kept for backwards compatibility)
# ---------------------------------------------------------------------------

def _race_to_ergast(race_row, include_results=False):
    """Convert a race row to Ergast Race object. Use raw_json when available for full structure."""
    r = dict(race_row)
    raw = r.get("raw_json")
    if raw:
        try:
            out = json.loads(raw)
            # Override date/time with start_override if set
            if r.get("start_override"):
                try:
                    dt = datetime.fromisoformat(r["start_override"].replace("Z", "+00:00"))
                    out["date"] = dt.strftime("%Y-%m-%d")
                    out["time"] = dt.strftime("%H:%M:%SZ")
                except Exception:
                    pass
        except json.JSONDecodeError:
            out = _race_to_ergast_minimal(r)
    else:
        out = _race_to_ergast_minimal(r)

    if include_results and r.get("has_results"):
        results = _get_results_for_race_ergast(r.get("season"), r.get("round"))
        if results:
            out["Results"] = results
    return out


def _race_to_ergast_minimal(r):
    """Build minimal Ergast Race from columns."""
    circuit = {
        "circuitId": r.get("circuit_id") or "",
        "url": r.get("circuit_url") or "",
        "circuitName": r.get("circuit_name") or "",
        "Location": {
            "lat": r.get("lat") or "",
            "long": r.get("long") or "",
            "locality": r.get("locality") or "",
            "country": r.get("country") or "",
        },
    }
    date = r.get("date") or ""
    time_part = r.get("time") or ""
    if r.get("start_override"):
        try:
            dt = datetime.fromisoformat(r["start_override"].replace("Z", "+00:00"))
            date = dt.strftime("%Y-%m-%d")
            time_part = dt.strftime("%H:%M:%SZ")
        except Exception:
            pass
    return {
        "season": r.get("season", ""),
        "round": r.get("round", ""),
        "url": r.get("race_url") or "",
        "raceName": r.get("race_name") or "",
        "Circuit": circuit,
        "date": date,
        "time": time_part,
    }


def _driver_to_ergast(drv_row):
    """Convert a driver row to Ergast Driver object."""
    return {
        "driverId": drv_row.get("driver_id", ""),
        "permanentNumber": drv_row.get("permanent_number") or "",
        "code": drv_row.get("code") or "",
        "url": drv_row.get("url") or "",
        "givenName": drv_row.get("given_name") or "",
        "familyName": drv_row.get("family_name") or "",
        "dateOfBirth": drv_row.get("date_of_birth") or "",
        "nationality": drv_row.get("nationality") or "",
    }


def _get_results_for_race_ergast(season, round_no):
    """Build Ergast Results array for a race from podium (p1,p2,p3)."""
    db = get_db()
    race = db.execute(
        "SELECT * FROM races WHERE season = ? AND round = ?",
        (season, round_no),
    ).fetchone()
    if not race or not race["has_results"]:
        return []

    p1, p2, p3 = race["p1_driver_id"], race["p2_driver_id"], race["p3_driver_id"]
    if not any((p1, p2, p3)):
        return []

    drivers_by_id = {}
    for did in (p1, p2, p3):
        if did:
            cur = db.execute(
                "SELECT * FROM drivers WHERE season = ? AND driver_id = ?",
                (season, did),
            )
            row = cur.fetchone()
            if row:
                drivers_by_id[did] = dict(row)

    points_map = {"1": "26", "2": "18", "3": "15"}
    results = []
    for pos, did in enumerate([p1, p2, p3], 1):
        if not did or did not in drivers_by_id:
            continue
        drv = drivers_by_id[did]
        results.append({
            "number": drv.get("permanent_number") or str(pos),
            "position": str(pos),
            "positionText": str(pos),
            "points": points_map.get(str(pos), "0"),
            "Driver": _driver_to_ergast(drv),
            "Constructor": PLACEHOLDER_CONSTRUCTOR,
            "grid": str(pos),
            "laps": "57",
            "status": "Finished",
            "Time": {"millis": "", "time": f"+{pos}.000"} if pos > 1 else {"millis": "0", "time": "1:30:00.000"},
        })
    return results


def _mrdata_wrapper(race_table_key: str, content: dict, season: str = "", round_no: str = ""):
    """Wrap content in MRData.RaceTable/DriverTable structure."""
    path_parts = [season]
    if round_no:
        path_parts.append(round_no)
    path = "/".join(path_parts)
    url = urljoin(app.config['ERGAST_BASE'], f"{path}.json") if path else ""
    total = len(content.get("Races", content.get("Drivers", [])))
    return {
        "MRData": {
            "xmlns": "",
            "series": "f1",
            "url": url,
            "limit": "30",
            "offset": "0",
            "total": str(total),
            race_table_key: {"season": season, **content} if season else content,
        },
    }


@app.route("/")
def index():
    """Redirect to admin."""
    return redirect(url_for("admin"))


@app.route("/health")
def health():
    """Health check."""
    return jsonify({"status": "ok"})


@app.route("/<season>.json")
def api_season_races(season: str):
    """GET /{season}.json - List all races for season (Ergast)."""
    db = get_db()
    rows = db.execute(
        "SELECT * FROM races WHERE season = ? ORDER BY round",
        (season,),
    ).fetchall()
    races = [_race_to_ergast(dict(r), include_results=False) for r in rows]
    wrap = _mrdata_wrapper("RaceTable", {"Races": races}, season=season)
    return jsonify(wrap)


@app.route("/<season>/drivers.json")
def api_season_drivers(season: str):
    """GET /{season}/drivers.json - List all drivers for season (Ergast)."""
    db = get_db()
    rows = db.execute(
        "SELECT * FROM drivers WHERE season = ? ORDER BY family_name",
        (season,),
    ).fetchall()
    drivers = [_driver_to_ergast(dict(r)) for r in rows]
    wrap = {"MRData": {"DriverTable": {"season": season, "Drivers": drivers}}}
    wrap["MRData"]["xmlns"] = ""
    wrap["MRData"]["series"] = "f1"
    wrap["MRData"]["url"] = urljoin(app.config['ERGAST_BASE'], f"{season}/drivers.json")
    wrap["MRData"]["limit"] = "30"
    wrap["MRData"]["offset"] = "0"
    wrap["MRData"]["total"] = str(len(drivers))
    return jsonify(wrap)


@app.route("/<season>/<round_no>/results.json")
def api_race_results(season: str, round_no: str):
    """GET /{season}/{round}/results.json - Race results if finished (Ergast)."""
    db = get_db()
    race = db.execute(
        "SELECT * FROM races WHERE season = ? AND round = ?",
        (season, round_no),
    ).fetchone()
    if not race:
        race_dict = {"season": season, "round": round_no, "Races": []}
    else:
        race_dict = _race_to_ergast(dict(race), include_results=True)
        race_dict = {"season": season, "round": round_no, "Races": [race_dict]}
    wrap = {"MRData": {"RaceTable": race_dict}}
    wrap["MRData"]["xmlns"] = ""
    wrap["MRData"]["series"] = "f1"
    wrap["MRData"]["url"] = urljoin(app.config['ERGAST_BASE'], f"{season}/{round_no}/results.json")
    wrap["MRData"]["limit"] = "30"
    wrap["MRData"]["offset"] = "0"
    wrap["MRData"]["total"] = str(len(race_dict.get("Races", [])))
    return jsonify(wrap)


# ---------------------------------------------------------------------------
# Admin routes
# ---------------------------------------------------------------------------

@app.route("/admin")
def admin():
    """Admin UI - list races with controls."""
    db = get_db()
    seasons = [r[0] for r in db.execute("SELECT season FROM seasons ORDER BY season DESC").fetchall()]
    if not seasons:
        seasons = [str(app.config['DEFAULT_SEASON'])]
    season = request.args.get("season", seasons[0])

    races = db.execute(
        "SELECT * FROM races WHERE season = ? ORDER BY round",
        (season,),
    ).fetchall()
    drivers = db.execute(
        "SELECT * FROM drivers WHERE season = ? ORDER BY family_name",
        (season,),
    ).fetchall()

    # Format start_override for datetime-local input (YYYY-MM-DDTHH:mm)
    race_list = []
    for r in races:
        d = dict(r)
        so = d.get("start_override")
        if so:
            try:
                dt = datetime.fromisoformat(so.replace("Z", "+00:00"))
                d["start_override_input"] = dt.strftime("%Y-%m-%dT%H:%M")
            except Exception:
                d["start_override_input"] = so[:16] if len(so) >= 16 else so
        else:
            d["start_override_input"] = ""
        race_list.append(d)

    return render_template(
        "admin.html",
        races=race_list,
        drivers=[dict(d) for d in drivers],
        seasons=seasons,
        current_season=season,
    )


@app.route("/admin/race/<int:race_id>/start", methods=["POST"])
def admin_set_start(race_id: int):
    """Set race start time (datetime)."""
    override = request.form.get("start_override", "").strip()
    db = get_db()
    db.execute("UPDATE races SET start_override = ? WHERE id = ?", (override or None, race_id))
    db.commit()
    flash("Start time updated" if override else "Start time cleared")
    return redirect(url_for("admin", season=request.form.get("season", "")))


@app.route("/admin/race/<int:race_id>/finish", methods=["POST"])
def admin_finish_race(race_id: int):
    """Mark race as finished (results endpoint will return data)."""
    db = get_db()
    db.execute("UPDATE races SET has_results = 1 WHERE id = ?", (race_id,))
    db.commit()
    flash("Race marked as finished")
    return redirect(url_for("admin", season=request.form.get("season", "")))


@app.route("/admin/race/<int:race_id>/unfinish", methods=["POST"])
def admin_unfinish_race(race_id: int):
    """Clear results so results endpoint returns no data."""
    db = get_db()
    db.execute(
        "UPDATE races SET has_results = 0, p1_driver_id = NULL, p2_driver_id = NULL, p3_driver_id = NULL WHERE id = ?",
        (race_id,),
    )
    db.commit()
    flash("Race results cleared")
    return redirect(url_for("admin", season=request.form.get("season", "")))


@app.route("/admin/reseed", methods=["POST"])
def admin_reseed():
    """Clear DB and re-seed from real Ergast API."""
    db = get_db()
    db.execute("DELETE FROM races")
    db.execute("DELETE FROM drivers")
    db.execute("DELETE FROM seasons")
    db.commit()
    season = str(app.config['DEFAULT_SEASON'])
    if _seed_season(season):
        flash(f"Reseeded season {season} from real API")
    else:
        flash("Reseed failed", "error")
    return redirect(url_for("admin", season=season))


@app.route("/admin/race/<int:race_id>/podium", methods=["POST"])
def admin_set_podium(race_id: int):
    """Set P1, P2, P3 from driver dropdowns."""
    p1 = request.form.get("p1_driver_id", "").strip() or None
    p2 = request.form.get("p2_driver_id", "").strip() or None
    p3 = request.form.get("p3_driver_id", "").strip() or None
    db = get_db()
    db.execute(
        "UPDATE races SET p1_driver_id = ?, p2_driver_id = ?, p3_driver_id = ? WHERE id = ?",
        (p1, p2, p3, race_id),
    )
    db.commit()
    flash("Podium updated")
    return redirect(url_for("admin", season=request.form.get("season", "")))


# --- Startup ---

with app.app_context():
    init_db()
    seed_if_empty()
