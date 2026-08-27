#!/usr/bin/env python3
"""
F1 Prediction App - A no-signup F1 prediction web app
Users enter a username, pick P1/P2/P3 for each race, and accumulate points.

All data (drivers, races, results) is pulled from the F1 API.
Votes lock when the race starts. Results are user-triggered via a button with browser auto-retry.
"""

import os
import uuid
import secrets
import sqlite3
import requests
from datetime import datetime, timezone, timedelta

from functools import wraps
from urllib.parse import urlencode

import click
from flask import Flask, render_template, request, redirect, url_for, session, g, flash, jsonify

import openf1
import fetch_attempts

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
app.config['DATABASE'] = os.environ.get('DATABASE_PATH', '/data/f1_predictions.db')
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=30)

# Environment configuration
app.config['ENVIRONMENT'] = os.environ.get('ENVIRONMENT', 'dev')
app.config['API_BASE_URL'] = os.environ.get('API_BASE_URL', '')
app.config['USE_STUB_API'] = os.environ.get('USE_STUB_API', 'false').lower() == 'true'
app.config['OPENF1_API_URL'] = openf1.OPENF1_BASE_URL
app.config['F1_SEASON'] = int(os.environ.get('F1_SEASON', '2026'))
app.config['DRIVER_REFRESH_SECRET'] = os.environ.get('DRIVER_REFRESH_SECRET', '')
RESULTS_CHECK_DELAY_MIN = 90  # Only check races that started 90+ min ago
MAX_RETRIES = 10
RETRY_INTERVAL_SEC = 120

# Live leaderboard configuration
LIVE_REFRESH_INTERVAL_SEC = 30  # Auto-refresh every 30 seconds
LIVE_RATE_LIMIT_SEC = 10  # Minimum time between API calls
LIVE_CACHE_TTL_SEC = 5  # Cache live data for 5 seconds

# Magic-link authentication configuration
LOGIN_TOKEN_BYTES = 32  # URL-safe token length
LOGIN_TOKEN_TTL_MINUTES = 15  # Token validity window

# Google OAuth configuration
GOOGLE_AUTH_ENDPOINT = 'https://accounts.google.com/o/oauth2/v2/auth'
GOOGLE_TOKEN_ENDPOINT = 'https://oauth2.googleapis.com/token'
GOOGLE_USERINFO_ENDPOINT = 'https://openidconnect.googleapis.com/v1/userinfo'
GOOGLE_OAUTH_SCOPE = 'openid email profile'

app.config['GOOGLE_OAUTH_CLIENT_ID'] = os.environ.get('GOOGLE_OAUTH_CLIENT_ID', '')
app.config['GOOGLE_OAUTH_CLIENT_SECRET'] = os.environ.get('GOOGLE_OAUTH_CLIENT_SECRET', '')
app.config['GOOGLE_OAUTH_REDIRECT_URI'] = os.environ.get(
    'GOOGLE_OAUTH_REDIRECT_URI',
    ''
)

# Simple in-memory cache for live data (keyed by race_id)
_live_data_cache = {}

def auto_lock_races():
    """Set status to 'locked' for races that have started (open + date in past)."""
    try:
        db = get_db()
        cutoff = _now_utc().strftime('%Y-%m-%d %H:%M:%S')
        db.execute('''
            UPDATE races SET status = 'locked'
            WHERE status = 'open' AND datetime(date) < datetime(?)
        ''', (cutoff,))
        db.commit()
    except Exception as e:
        app.logger.warning(f"auto_lock_races: {e}")


# Context processor to make environment available to all templates
@app.context_processor
def inject_environment():
    return dict(
        environment=app.config['ENVIRONMENT'],
        api_base_url=app.config['API_BASE_URL'],
        use_stub_api=app.config['USE_STUB_API'],
        app_version=os.environ.get('APP_VERSION', ''),
        f1_season=app.config['F1_SEASON']
    )


@app.context_processor
def inject_current_user():
    """Make the current user available to all templates as current_user."""
    return dict(current_user=get_current_user())

# Database helpers
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

def _parse_race_datetime(date_str):
    """Parse race date string (YYYY-MM-DD HH:MM:SS or with Z) to timezone-aware UTC datetime."""
    if not date_str:
        return None
    try:
        s = str(date_str).strip().replace('Z', '').strip()[:19]
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None

def _now_utc():
    return datetime.now(timezone.utc)

def compute_race_status(race, has_results):
    """
    Compute race status from race start time and results.
    Votes lock the minute the race starts (race_start <= now).
    """
    if has_results:
        return 'completed'
    stored = race.get('status') or ''
    if stored == 'locked':
        return 'locked'
    race_start = _parse_race_datetime(race.get('date', ''))
    if race_start and race_start <= _now_utc():
        return 'locked'
    return 'open'

def enrich_race_with_status(race_dict, has_results):
    """Add computed status to a race dict."""
    r = dict(race_dict)
    r['status'] = compute_race_status(r, has_results)
    return r

def init_db():
    """Initialize database with schema."""
    db = get_db()

    # Users table
    db.execute('''
        CREATE TABLE IF NOT EXISTS users (
            session_id TEXT PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            is_synthetic INTEGER DEFAULT 0,
            persona TEXT,
            email TEXT UNIQUE,
            legacy_user INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Magic-link login tokens
    db.execute('''
        CREATE TABLE IF NOT EXISTS login_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            token TEXT NOT NULL UNIQUE,
            used INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP NOT NULL
        )
    ''')

    # Drivers table - populated from API
    db.execute('''
        CREATE TABLE IF NOT EXISTS drivers (
            id INTEGER PRIMARY KEY,
            driver_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            team TEXT,
            number INTEGER NOT NULL,
            code TEXT,
            nationality TEXT
        )
    ''')

    # Metadata table for tracking refreshes
    db.execute('''
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Races table - date is race start (UTC)
    db.execute('''
        CREATE TABLE IF NOT EXISTS races (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            round INTEGER NOT NULL,
            date TIMESTAMP NOT NULL,
            status TEXT DEFAULT 'open' CHECK (status IN ('upcoming', 'open', 'locked', 'completed'))
        )
    ''')

    # Leagues table (F1-20 / BUD-150) - a league is a view over the global
    # game, scoped to a scoring window. start_round is season-relative
    # (races.round), NULL when whole_season is set.
    db.execute('''
        CREATE TABLE IF NOT EXISTS leagues (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            emoji_or_color TEXT NOT NULL,
            start_round INTEGER,
            whole_season BOOLEAN DEFAULT 0,
            admin_user_id TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (admin_user_id) REFERENCES users(session_id)
        )
    ''')

    # League membership - the creating admin is inserted here immediately
    # (BUD-150); BUD-151 adds the invite/join flow on top of this shape.
    db.execute('''
        CREATE TABLE IF NOT EXISTS league_members (
            league_id INTEGER NOT NULL,
            user_id TEXT NOT NULL,
            joined_at_round INTEGER,
            is_admin BOOLEAN DEFAULT 0,
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (league_id, user_id),
            FOREIGN KEY (league_id) REFERENCES leagues(id),
            FOREIGN KEY (user_id) REFERENCES users(session_id)
        )
    ''')

    # Predictions table
    db.execute('''
        CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            race_id INTEGER NOT NULL,
            p1_driver_id INTEGER NOT NULL,
            p2_driver_id INTEGER NOT NULL,
            p3_driver_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(session_id),
            FOREIGN KEY (race_id) REFERENCES races(id),
            UNIQUE(user_id, race_id)
        )
    ''')

    # Results table
    db.execute('''
        CREATE TABLE IF NOT EXISTS results (
            race_id INTEGER PRIMARY KEY,
            p1_driver_id INTEGER NOT NULL,
            p2_driver_id INTEGER NOT NULL,
            p3_driver_id INTEGER NOT NULL,
            FOREIGN KEY (race_id) REFERENCES races(id)
        )
    ''')

    # Scores table
    db.execute('''
        CREATE TABLE IF NOT EXISTS scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            race_id INTEGER NOT NULL,
            points INTEGER NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(session_id),
            FOREIGN KEY (race_id) REFERENCES races(id),
            UNIQUE(user_id, race_id)
        )
    ''')

    # Race-weekend state machine (used by cron/race_manager.py)
    db.execute('''
        CREATE TABLE IF NOT EXISTS race_stages (
            race_id      INTEGER PRIMARY KEY,
            stage        TEXT    NOT NULL
                         CHECK (stage IN ('watching','locked','polling','completed')),
            entered_at   TEXT    NOT NULL,
            last_poll_at TEXT,
            poll_count   INTEGER DEFAULT 0,
            FOREIGN KEY (race_id) REFERENCES races(id)
        )
    ''')

    # Last-known-good cache for upstream reads (F1-02)
    openf1.ensure_cache_table(db)

    # Fetch-attempt observability (F1-03 / BUD-125)
    fetch_attempts.ensure_fetch_attempts_table(db)

    _apply_migrations(db)

    db.commit()

    # Lazy load from API on first startup
    ensure_drivers_loaded(db)
    ensure_races_loaded(db)


def _column_names(db, table):
    return {row[1] for row in db.execute(f'PRAGMA table_info({table})').fetchall()}


def _apply_migrations(db):
    """Additive schema migrations for databases created before a column existed.

    CREATE TABLE IF NOT EXISTS silently does nothing on an existing table, so
    new columns need an explicit ALTER. Every migration here is additive and
    idempotent — safe to run on every startup.
    """
    users = _column_names(db, 'users')
    if 'email' not in users:
        # SQLite forbids UNIQUE in ALTER TABLE ADD COLUMN (only CREATE TABLE
        # allows it) — add the column plain, then enforce uniqueness via a
        # separate index. This would have crashed on first startup against
        # any real pre-existing users table.
        db.execute('ALTER TABLE users ADD COLUMN email TEXT')
        db.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users(email)')
        app.logger.info('Migration: users.email added')

    if 'legacy_user' not in users:
        db.execute('ALTER TABLE users ADD COLUMN legacy_user INTEGER DEFAULT 0')
        db.execute('''
            UPDATE users SET legacy_user = 1
            WHERE email IS NULL OR email = ''
        ''')
        app.logger.info('Migration: users.legacy_user added and flagged existing anonymous users')

    races = _column_names(db, 'races')
    if 'session_key' not in races:
        # OpenF1 keys results by session_key; without it we cannot fetch results.
        db.execute('ALTER TABLE races ADD COLUMN session_key INTEGER')
        app.logger.info('Migration: races.session_key added')

    results = _column_names(db, 'results')
    for column, ddl in (
        ('had_safety_car', 'ALTER TABLE results ADD COLUMN had_safety_car INTEGER'),
        ('safety_car_count', 'ALTER TABLE results ADD COLUMN safety_car_count INTEGER'),
        ('had_virtual_safety_car', 'ALTER TABLE results ADD COLUMN had_virtual_safety_car INTEGER'),
        ('virtual_safety_car_count', 'ALTER TABLE results ADD COLUMN virtual_safety_car_count INTEGER'),
        ('data_source', 'ALTER TABLE results ADD COLUMN data_source TEXT'),
        ('recorded_at', 'ALTER TABLE results ADD COLUMN recorded_at TIMESTAMP'),
    ):
        if column not in results:
            db.execute(ddl)
            app.logger.info('Migration: results.%s added', column)

    users = _column_names(db, 'users')
    if 'is_synthetic' not in users:
        db.execute('ALTER TABLE users ADD COLUMN is_synthetic INTEGER DEFAULT 0')
        app.logger.info('Migration: users.is_synthetic added')
    if 'persona' not in users:
        db.execute('ALTER TABLE users ADD COLUMN persona TEXT')
        app.logger.info('Migration: users.persona added')

# --- API fetching ---

def fetch_drivers_from_api(db=None):
    """Fetch the current driver grid from OpenF1."""
    try:
        drivers_raw = openf1.get_drivers(season=app.config['F1_SEASON'], db=db).data
    except openf1.OpenF1Error as e:
        app.logger.error(f"Failed to fetch drivers from OpenF1: {e}")
        return None

    drivers = []
    for idx, driver in enumerate(drivers_raw, start=1):
        number = driver.get('driver_number')
        if number is None:
            continue
        name = openf1.driver_display_name(driver)
        drivers.append({
            'id': idx,
            # OpenF1 has no stable slug; derive one so existing driver_id
            # semantics (unique text key) still hold.
            'driver_id': (driver.get('name_acronym') or name).lower().replace(' ', '_'),
            'name': name,
            'number': int(number),
            'code': driver.get('name_acronym'),
            'nationality': driver.get('country_code'),
            'team': driver.get('team_name'),
        })
    return drivers or None

def ensure_drivers_loaded(db):
    """Ensure drivers are loaded from API. Called on startup if empty."""
    count = db.execute('SELECT COUNT(*) FROM drivers').fetchone()[0]

    if count == 0:
        app.logger.info("No drivers found - fetching from API...")
        drivers = fetch_drivers_from_api(db)

        if drivers:
            for driver in drivers:
                db.execute('''
                    INSERT INTO drivers (id, driver_id, name, team, number, code, nationality)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (driver['id'], driver['driver_id'], driver['name'],
                      driver['team'], driver['number'], driver['code'], driver['nationality']))

            db.execute('''
                INSERT INTO metadata (key, value, updated_at)
                VALUES ('drivers_last_refresh', ?, CURRENT_TIMESTAMP)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
            ''', (datetime.now().isoformat(),))

            db.commit()
            app.logger.info(f"Loaded {len(drivers)} drivers from API")
        else:
            app.logger.error("Failed to load drivers from API - app may not function correctly")

def fetch_races_from_api(db=None):
    """
    Fetch the race calendar from OpenF1.

    Returns (name, round, date_str, session_key) tuples. session_key is what
    results are later fetched with, so it is stored alongside the race.
    """
    season = app.config['F1_SEASON']
    try:
        sessions = openf1.get_race_sessions(season=season, db=db).data
    except openf1.OpenF1Error as e:
        app.logger.error(f"Failed to fetch races from OpenF1: {e}")
        return None

    # Meeting names give "Australian Grand Prix" rather than "Melbourne".
    meeting_names = {}
    try:
        for m in openf1.get_meetings(season=season, db=db).data:
            meeting_names[m.get('meeting_key')] = m.get('meeting_name') or m.get('meeting_official_name')
    except openf1.OpenF1Error:
        app.logger.warning("Meeting names unavailable; falling back to circuit/country names")

    races = []
    for session_data in sessions:
        name = (meeting_names.get(session_data.get('meeting_key'))
                or session_data.get('circuit_short_name')
                or session_data.get('country_name')
                or 'Unknown')
        date_start = session_data.get('date_start')
        if not date_start:
            continue
        # Store naive UTC to match the existing datetime() comparisons in SQL.
        date_str = date_start.replace('T', ' ')[:19]
        races.append((name, session_data['round'], date_str, session_data.get('session_key')))

    return races or None

def ensure_races_loaded(db):
    """Ensure races are loaded from API. Called on startup if empty. No fallback."""
    count = db.execute('SELECT COUNT(*) FROM races').fetchone()[0]

    if count > 0:
        return

    app.logger.info("No races found - fetching from API...")
    races = fetch_races_from_api(db)

    if not races:
        app.logger.error("Failed to fetch races from API - race table will remain empty")
        return

    for name, round_num, date_str, session_key in races:
        db.execute(
            'INSERT INTO races (name, round, date, status, session_key) VALUES (?, ?, ?, ?, ?)',
            (name, round_num, date_str, 'open', session_key)
        )

    db.commit()
    app.logger.info(f"Loaded {len(races)} races from API")

def refresh_drivers_from_api(db):
    """Refresh drivers from API. Called by CronJob."""
    app.logger.info("Refreshing drivers from API...")

    drivers = fetch_drivers_from_api()
    if not drivers:
        return False, "Failed to fetch from API"

    old_drivers = {r['driver_id']: r['id'] for r in db.execute('SELECT driver_id, id FROM drivers').fetchall()}
    id_mapping = {}
    new_id = 1

    for driver in drivers:
        old_id = old_drivers.get(driver['driver_id'])
        if old_id:
            id_mapping[old_id] = new_id
        driver['new_id'] = new_id
        new_id += 1

    # Update predictions to use new driver IDs before deleting old drivers
    # Build CASE expressions for each column to avoid order-of-updates issue
    p1_cases = " ".join(f"WHEN {old_id} THEN {new_id}" for old_id, new_id in id_mapping.items())
    p2_cases = p1_cases
    p3_cases = p1_cases

    if id_mapping:
        db.execute(f'''
            UPDATE predictions
            SET p1_driver_id = CASE p1_driver_id {p1_cases} ELSE p1_driver_id END,
                p2_driver_id = CASE p2_driver_id {p2_cases} ELSE p2_driver_id END,
                p3_driver_id = CASE p3_driver_id {p3_cases} ELSE p3_driver_id END
        ''')

    db.execute('DELETE FROM drivers')

    for driver in drivers:
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, team, number, code, nationality)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (driver['new_id'], driver['driver_id'], driver['name'],
              driver['team'], driver['number'], driver['code'], driver['nationality']))

    db.execute('''
        INSERT INTO metadata (key, value, updated_at)
        VALUES ('drivers_last_refresh', ?, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
    ''', (datetime.now().isoformat(),))

    db.commit()
    app.logger.info(f"Refreshed {len(drivers)} drivers")
    return True, f"Refreshed {len(drivers)} drivers"

# --- Results checking (user-triggered with browser auto-retry) ---

def _race_session_key(db, race):
    """
    session_key for a race, resolving lazily for rows seeded before the column
    existed (or seeded by fixtures). Returns None when it cannot be resolved.
    """
    key = race['session_key'] if 'session_key' in race.keys() else None
    if key:
        return key
    try:
        for session_data in openf1.get_race_sessions(season=app.config['F1_SEASON'], db=db).data:
            if session_data.get('round') == race['round']:
                db.execute('UPDATE races SET session_key = ? WHERE id = ?',
                           (session_data['session_key'], race['id']))
                db.commit()
                return session_data['session_key']
    except openf1.OpenF1Error as e:
        app.logger.warning(f"Could not resolve session_key for race {race['id']}: {e}")
    return None


def fetch_race_results_from_api(db, race):
    """
    Podium for a race from OpenF1, as DB driver ids.

    Returns None when the race has not finished (or the drivers cannot be
    matched), which callers treat as "try again later" rather than an error.
    """
    session_key = _race_session_key(db, race)
    if not session_key:
        return None
    try:
        podium = openf1.get_podium(session_key, db=db)
    except openf1.OpenF1Error as e:
        fetch_attempts.record_fetch_attempt(
            db, 'session_result', f'session_result?session_key={session_key}',
            fetch_attempts.outcome_for_error(e),
            http_status=getattr(e, 'status_code', None),
            session_key=session_key, race_id=race['id'], detail=str(e),
        )
        app.logger.warning(f"Results fetch failed for race {race['id']}: {e}")
        return None
    if not podium:
        fetch_attempts.record_fetch_attempt(
            db, 'session_result', f'session_result?session_key={session_key}',
            fetch_attempts.Outcome.EMPTY, session_key=session_key, race_id=race['id'],
        )
        return None
    fetch_attempts.record_fetch_attempt(
        db, 'session_result', f'session_result?session_key={session_key}',
        fetch_attempts.Outcome.OK, session_key=session_key, race_id=race['id'],
    )

    resolved = {}
    for slot in ('p1', 'p2', 'p3'):
        driver_db_id = get_driver_db_id_by_number(db, podium[slot]['driver_number'])
        if driver_db_id is None:
            app.logger.warning(
                "Race %s: driver #%s (%s) not in drivers table — skipping ingest",
                race['id'], podium[slot]['driver_number'], podium[slot]['driver_name'])
            return None
        resolved[f'{slot}_driver_id'] = driver_db_id

    try:
        resolved['safety_car'] = openf1.get_safety_car_summary(session_key, db=db)
    except openf1.OpenF1Error:
        resolved['safety_car'] = None
    return resolved


def get_driver_db_id_by_number(db, driver_number):
    """DB driver id from a car number (how OpenF1 identifies drivers)."""
    if driver_number is None:
        return None
    row = db.execute('SELECT id FROM drivers WHERE number = ?', (driver_number,)).fetchone()
    return row['id'] if row else None


def get_races_pending_results(db, min_minutes_after_start=RESULTS_CHECK_DELAY_MIN):
    """Races that started min_minutes_after_start+ ago, no results in DB."""
    cutoff = _now_utc() - timedelta(minutes=min_minutes_after_start)
    cutoff_str = cutoff.strftime('%Y-%m-%d %H:%M:%S')
    return db.execute('''
        SELECT r.id, r.name, r.round, r.date
        FROM races r
        LEFT JOIN results res ON r.id = res.race_id
        WHERE res.race_id IS NULL AND r.date <= ?
        ORDER BY r.round
    ''', (cutoff_str,)).fetchall()

def check_and_ingest_results(db):
    """
    Check F1 API for races pending results and ingest if available.
    Returns (updated_races, error_message).
    """
    season = app.config['F1_SEASON']
    pending = get_races_pending_results(db)
    updated = []

    for race in pending:
        podium = fetch_race_results_from_api(db, race)
        if not podium:
            continue

        p1_id = podium['p1_driver_id']
        p2_id = podium['p2_driver_id']
        p3_id = podium['p3_driver_id']

        sc = podium.get('safety_car') or {}
        db.execute('''
            INSERT INTO results (race_id, p1_driver_id, p2_driver_id, p3_driver_id,
                                 had_safety_car, safety_car_count,
                                 had_virtual_safety_car, virtual_safety_car_count,
                                 data_source, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'openf1', CURRENT_TIMESTAMP)
        ''', (race['id'], p1_id, p2_id, p3_id,
              1 if sc.get('had_safety_car') else 0, sc.get('safety_car_count'),
              1 if sc.get('had_virtual_safety_car') else 0, sc.get('virtual_safety_car_count')))

        predictions = db.execute('SELECT * FROM predictions WHERE race_id = ?', (race['id'],)).fetchall()
        result_data = {'p1_driver_id': p1_id, 'p2_driver_id': p2_id, 'p3_driver_id': p3_id}

        for pred in predictions:
            points = calculate_score(dict(pred), result_data)
            db.execute('''
                INSERT INTO scores (user_id, race_id, points)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id, race_id) DO UPDATE SET points = excluded.points
            ''', (pred['user_id'], race['id'], points))

        updated.append(race['name'])

    if updated:
        db.commit()

    return updated, None

def has_races_pending_results(db):
    """True if any race is eligible for results check (started 90+ min ago, no results)."""
    return len(get_races_pending_results(db)) > 0

# --- Live Leaderboard (LL-001 to LL-011) ---

def _get_cached_live_data(race_id):
    """Get cached live data if still valid."""
    now = _now_utc()
    if race_id in _live_data_cache:
        cached = _live_data_cache[race_id]
        age = (now - cached['timestamp']).total_seconds()
        if age < LIVE_CACHE_TTL_SEC:
            return cached['data']
    return None


def _set_cached_live_data(race_id, data):
    """Cache live data with current timestamp."""
    _live_data_cache[race_id] = {
        'data': data,
        'timestamp': _now_utc()
    }


class LivePositions(list):
    """
    Live standings plus cache provenance (F1-02).

    Behaves exactly like the plain list of position dicts it always was —
    subclassing keeps every existing truthiness/indexing/iteration call site
    working unchanged — but callers that care can read `.from_cache`,
    `.is_stale` and `.age_label` to render a data-age indicator instead of
    silently serving last-known-good data as if it were live.
    """

    def __init__(self, positions, from_cache=False, is_stale=False,
                 age_label=None, fetched_at=None):
        super().__init__(positions)
        self.from_cache = from_cache
        self.is_stale = is_stale
        self.age_label = age_label
        self.fetched_at = fetched_at


def fetch_live_race_data(db, race):
    """
    Current classification for a race from OpenF1.

    OpenF1 publishes session_result during a session, so this doubles as the
    live standings feed. Returns None when nothing is published yet.
    """
    session_key = _race_session_key(db, race)
    if not session_key:
        return None
    try:
        result_cached = openf1.get_session_result(session_key, db=db)
        results = result_cached.data
        if not results:
            return None
        drivers = {d.get('driver_number'): d
                   for d in openf1.get_drivers(session_key=session_key, db=db).data}
    except openf1.OpenF1Error as e:
        app.logger.warning(f"Failed to fetch live race data: {e}")
        return None

    positions = []
    for idx, row in enumerate(results):
        driver = drivers.get(row.get('driver_number'), {})
        status = 'Finished'
        if row.get('dnf'):
            status = 'DNF'
        elif row.get('dns'):
            status = 'DNS'
        elif row.get('dsq'):
            status = 'DSQ'
        positions.append({
            'position': int(row.get('position') or idx + 1),
            'driver_id': (driver.get('name_acronym') or '').lower(),
            'driver_number': row.get('driver_number'),
            'name': openf1.driver_display_name(driver) if driver else f"#{row.get('driver_number')}",
            'code': driver.get('name_acronym', ''),
            'constructor': driver.get('team_name', ''),
            'nationality': driver.get('country_code') or '',
            'grid': '',
            'laps': row.get('number_of_laps', ''),
            'status': status,
            'points': row.get('points', 0) or 0,
            'fastest_lap': '',
        })
    return LivePositions(
        positions,
        from_cache=result_cached.from_cache,
        is_stale=result_cached.is_stale,
        age_label=result_cached.age_label() if result_cached.from_cache else None,
        fetched_at=result_cached.fetched_at,
    )


def calculate_projected_points(prediction, current_positions):
    """
    Calculate projected points based on current race positions.
    LL-003: Projected points calculation.
    
    Returns dict with:
    - p1_match: bool
    - p2_match: bool
    - p3_match: bool
    - exact_matches: int
    - driver_matches: int
    - projected_points: int
    """
    if not current_positions:
        return {
            'p1_match': False, 'p2_match': False, 'p3_match': False,
            'exact_matches': 0, 'driver_matches': 0, 'projected_points': 0
        }
    
    # Map driver_id to position for quick lookup
    driver_positions = {p['driver_id']: p['position'] for p in current_positions}
    
    pred_p1 = str(prediction['p1_driver_id'])
    pred_p2 = str(prediction['p2_driver_id'])
    pred_p3 = str(prediction['p3_driver_id'])
    
    # Get predicted drivers' current positions (or default to 99 if not found)
    p1_pos = driver_positions.get(pred_p1, 99)
    p2_pos = driver_positions.get(pred_p2, 99)
    p3_pos = driver_positions.get(pred_p3, 99)
    
    # Calculate points
    p1_match = p1_pos == 1
    p2_match = p2_pos == 2
    p3_match = p3_pos == 3
    
    exact_matches = sum([p1_match, p2_match, p3_match])
    
    # Count driver matches (correct driver in podium, wrong position)
    driver_matches = 0
    predicted_drivers = {pred_p1, pred_p2, pred_p3}
    podium_drivers = {p['driver_id'] for p in current_positions[:3] if p['position'] <= 3}
    
    for did in predicted_drivers:
        if did in podium_drivers:
            # Check if it's an exact match
            if did == pred_p1 and p1_match:
                continue
            if did == pred_p2 and p2_match:
                continue
            if did == pred_p3 and p3_match:
                continue
            driver_matches += 1
    
    # Calculate points: P1=10, P2=6, P3=4, +1 for driver in podium wrong position
    projected_points = 0
    if p1_match:
        projected_points += 10
    if p2_match:
        projected_points += 6
    if p3_match:
        projected_points += 4
    projected_points += driver_matches
    
    return {
        'p1_match': p1_match,
        'p2_match': p2_match,
        'p3_match': p3_match,
        'exact_matches': exact_matches,
        'driver_matches': driver_matches,
        'projected_points': projected_points,
        'p1_current_pos': p1_pos,
        'p2_current_pos': p2_pos,
        'p3_current_pos': p3_pos,
    }


def calculate_best_worst_case(prediction, current_positions):
    """
    Calculate best and worst case scenarios.
    LL-005: Best/worst case projection.
    
    Best case: All predicted drivers finish in predicted positions (20 points max)
    Worst case: Current points only (no more improvement possible)
    """
    if not current_positions:
        return {'best': 0, 'worst': 0, 'current': 0}
    
    driver_positions = {str(p['driver_id']): p['position'] for p in current_positions}
    
    # Map predicted drivers to their positions
    pred_p1 = str(prediction['p1_driver_id'])
    pred_p2 = str(prediction['p2_driver_id'])
    pred_p3 = str(prediction['p3_driver_id'])
    
    p1_curr = driver_positions.get(pred_p1, 99)
    p2_curr = driver_positions.get(pred_p2, 99)
    p3_curr = driver_positions.get(pred_p3, 99)
    
    # Calculate current points and remaining possible
    current_points = 0
    remaining_for_best = 0
    
    # P1 analysis: exact = 10, in podium wrong = 1, out = 0, remaining = 10
    if p1_curr == 1:
        current_points += 10
        remaining_for_best += 0  # Already exact
    elif p1_curr <= 3:
        current_points += 1  # Driver in podium but wrong position
        remaining_for_best += 9  # Could still get 9 more (10-1) if moves to P1
    else:
        remaining_for_best += 10  # Could still get 10 if gets P1
    
    # P2 analysis: exact = 6, in podium wrong = 1, out = 0, remaining = 6
    if p2_curr == 2:
        current_points += 6
        remaining_for_best += 0
    elif p2_curr <= 3:
        current_points += 1
        remaining_for_best += 5  # Could still get 5 more (6-1)
    else:
        remaining_for_best += 6  # Could still get 6 if gets P2
    
    # P3 analysis: exact = 4, in podium wrong = 1, out = 0, remaining = 4
    if p3_curr == 3:
        current_points += 4
        remaining_for_best += 0
    elif p3_curr <= 3:
        current_points += 1
        remaining_for_best += 3  # Could still get 3 more (4-1)
    else:
        remaining_for_best += 4  # Could still get 4 if gets P3
    
    # Best case: current points + remaining possible improvement
    best_case = current_points + remaining_for_best
    
    # Worst case: current points (no more improvement)
    worst_case = current_points
    
    return {
        'best': best_case,
        'worst': worst_case,
        'current': current_points
    }


def get_user_predictions_for_race(db, race_id):
    """Get all user predictions for a race with projected points."""
    predictions = db.execute('''
        SELECT p.*, u.username,
               d1.name as p1_name, d1.driver_id as p1_driver_id_api,
               d2.name as p2_name, d2.driver_id as p2_driver_id_api,
               d3.name as p3_name, d3.driver_id as p3_driver_id_api
        FROM predictions p
        JOIN users u ON p.user_id = u.session_id
        JOIN drivers d1 ON p.p1_driver_id = d1.id
        JOIN drivers d2 ON p.p2_driver_id = d2.id
        JOIN drivers d3 ON p.p3_driver_id = d1.id
        WHERE p.race_id = ?
        ORDER BY u.username
    ''', (race_id,)).fetchall()
    return predictions


# Admin: only these usernames can lock races, enter results, etc.
ADMIN_USERNAMES = {'brett'}


def is_admin(user):
    """Check if user is an admin (case-insensitive)."""
    return user and user['username'].strip().lower() in ADMIN_USERNAMES


def is_legacy_user(user):
    """Return True for existing anonymous users who still need email migration."""
    return user is not None and user['legacy_user'] and not user['email']


def admin_required(f):
    """Decorator: require admin user for lock/enter-results routes."""
    @wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user:
            flash('Please log in to access admin', 'error')
            return redirect(url_for('index'))
        if not is_admin(user):
            flash('Admin access only', 'error')
            return redirect(url_for('races'))
        return f(*args, **kwargs)
    return decorated


def get_current_user():
    """Get current user from session."""
    session_id = session.get('session_id')
    if not session_id:
        return None

    db = get_db()
    user = db.execute(
        'SELECT * FROM users WHERE session_id = ?', (session_id,)
    ).fetchone()
    return user


def _normalize_email(email):
    """Normalize email for storage and lookup."""
    return (email or '').strip().lower()


def _generate_login_token():
    """Generate a URL-safe one-time login token."""
    return secrets.token_urlsafe(LOGIN_TOKEN_BYTES)


def _login_token_url(token):
    """Build the relative magic-link URL for a token."""
    return url_for('login_verify', token=token, _external=False)


def create_login_token(email):
    """Create a magic-link token for the given email and log it for debug/tests.

    Returns the raw token string. In production this would send an email;
    here we log the link to stdout so tests and dev can retrieve it.
    """
    db = get_db()
    normalized = _normalize_email(email)
    token = _generate_login_token()
    expires_at = _now_utc() + timedelta(minutes=LOGIN_TOKEN_TTL_MINUTES)

    # Invalidate any prior unused token for this email before creating a new one.
    db.execute(
        'UPDATE login_tokens SET used = 1 WHERE email = ? AND used = 0',
        (normalized,)
    )
    db.execute('''
        INSERT INTO login_tokens (email, token, expires_at)
        VALUES (?, ?, ?)
    ''', (normalized, token, expires_at.strftime('%Y-%m-%d %H:%M:%S')))
    db.commit()

    link = _login_token_url(token)
    app.logger.info('MAGIC-LINK email=%s token=%s link=%s', normalized, token, link)
    return token


def _get_token_record(db, token):
    """Fetch a non-expired token record, or None if invalid/expired."""
    now = _now_utc().strftime('%Y-%m-%d %H:%M:%S')
    return db.execute('''
        SELECT * FROM login_tokens
        WHERE token = ? AND used = 0 AND expires_at > ?
    ''', (token, now)).fetchone()


def _find_user_by_email(db, email):
    """Fetch user by normalized email."""
    return db.execute(
        'SELECT * FROM users WHERE email = ?', (_normalize_email(email),)
    ).fetchone()


def consume_login_token(token):
    """Validate a login token and return the associated email.

    Returns the normalized email on success, or None if the token is invalid,
    expired, or already used. Marks the token as used on success.
    """
    db = get_db()
    record = _get_token_record(db, token)
    if not record:
        return None

    db.execute('UPDATE login_tokens SET used = 1 WHERE id = ?', (record['id'],))
    db.commit()
    return record['email']


def bind_email_to_session(email):
    """Ensure a user row exists for the email and attach it to the session.

    If a user with this email already exists, their session_id becomes the
    current session. Otherwise a new user is created (or the current anonymous
    user is upgraded when possible) and the email is bound to it.
    """
    db = get_db()
    normalized = _normalize_email(email)
    existing = _find_user_by_email(db, normalized)

    if existing:
        session['session_id'] = existing['session_id']
        return existing['session_id']

    current_session_id = session.get('session_id')
    current_user = None
    if current_session_id:
        current_user = db.execute(
            'SELECT * FROM users WHERE session_id = ?', (current_session_id,)
        ).fetchone()

    if current_user and not current_user['email']:
        # Upgrade anonymous user in-place; username stays the same.
        try:
            db.execute(
                'UPDATE users SET email = ?, legacy_user = 0 WHERE session_id = ?',
                (normalized, current_session_id)
            )
            db.commit()
            return current_session_id
        except sqlite3.IntegrityError:
            # Race: another session claimed the email; fall through to create new.
            pass

    # Create a fresh user bound to this email.
    new_session_id = str(uuid.uuid4())
    # Derive a username from the email local part; ensure uniqueness.
    base_username = normalized.split('@')[0] or 'user'
    username = base_username
    attempt = 1
    while True:
        try:
            db.execute('''
                INSERT INTO users (session_id, username, email, legacy_user)
                VALUES (?, ?, ?, ?)
            ''', (new_session_id, username, normalized, 0))
            db.commit()
            break
        except sqlite3.IntegrityError:
            username = f"{base_username}_{attempt}"
            attempt += 1

    session['session_id'] = new_session_id
    session.permanent = True
    return new_session_id


def _get_google_oauth_redirect_uri():
    """Return the configured Google OAuth redirect URI, or derive one."""
    configured = app.config.get('GOOGLE_OAUTH_REDIRECT_URI', '')
    if configured:
        return configured
    return url_for('login_oauth_google_callback', _external=True)


def _generate_oauth_state():
    """Generate a short-lived CSRF state token for the OAuth flow."""
    return secrets.token_urlsafe(16)


def _google_auth_url(state):
    """Build the Google OAuth authorization request URL."""
    params = {
        'client_id': app.config['GOOGLE_OAUTH_CLIENT_ID'],
        'redirect_uri': _get_google_oauth_redirect_uri(),
        'response_type': 'code',
        'scope': GOOGLE_OAUTH_SCOPE,
        'state': state,
        'access_type': 'online',
    }
    return f'{GOOGLE_AUTH_ENDPOINT}?{urlencode(params)}'


def _exchange_google_code(code):
    """Exchange an authorization code for Google OAuth tokens.

    Returns the JSON token response on success, or None on failure.
    """
    try:
        response = requests.post(
            GOOGLE_TOKEN_ENDPOINT,
            data={
                'code': code,
                'client_id': app.config['GOOGLE_OAUTH_CLIENT_ID'],
                'client_secret': app.config['GOOGLE_OAUTH_CLIENT_SECRET'],
                'redirect_uri': _get_google_oauth_redirect_uri(),
                'grant_type': 'authorization_code',
            },
            timeout=10,
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        app.logger.warning(f'Google token exchange failed: {e}')
        return None


def _fetch_google_userinfo(access_token):
    """Fetch the user's Google profile via the OAuth userinfo endpoint.

    Returns a dict with at least an 'email' key on success, or None.
    """
    try:
        response = requests.get(
            GOOGLE_USERINFO_ENDPOINT,
            headers={'Authorization': f'Bearer {access_token}'},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        app.logger.warning(f'Google userinfo fetch failed: {e}')
        return None


def calculate_score(prediction, result):
    """Calculate score for a prediction."""
    points = 0

    if prediction['p1_driver_id'] == result['p1_driver_id']:
        points += 10
    if prediction['p2_driver_id'] == result['p2_driver_id']:
        points += 6
    if prediction['p3_driver_id'] == result['p3_driver_id']:
        points += 4

    predicted_drivers = {prediction['p1_driver_id'], prediction['p2_driver_id'], prediction['p3_driver_id']}
    result_drivers = {result['p1_driver_id'], result['p2_driver_id'], result['p3_driver_id']}

    for driver_id in predicted_drivers:
        if driver_id in result_drivers:
            exact = (
                (driver_id == prediction['p1_driver_id'] and driver_id == result['p1_driver_id']) or
                (driver_id == prediction['p2_driver_id'] and driver_id == result['p2_driver_id']) or
                (driver_id == prediction['p3_driver_id'] and driver_id == result['p3_driver_id'])
            )
            if not exact:
                points += 1

    return points

def race_slug(race):
    """Derive URL slug from race, e.g. 2026_chinese from 'Chinese Grand Prix'."""
    season = app.config['F1_SEASON']
    name = (race.get('name') or '').strip()
    # "Chinese Grand Prix" -> "Chinese", "Saudi Arabian Grand Prix" -> "Saudi Arabian"
    for suffix in (' Grand Prix', ' GP'):
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()
            break
    # "Saudi Arabian" -> "saudi_arabian", "Chinese" -> "chinese"
    slug = name.lower().replace(' ', '_').replace('-', '_')
    slug = ''.join(c for c in slug if c.isalnum() or c == '_')
    return f"{season}_{slug}" if slug else f"{season}_round{race.get('round', 0)}"


def get_races_with_computed_status(db):
    """Fetch all races and enrich with computed status."""
    races = db.execute('''
        SELECT r.*, res.p1_driver_id, res.p2_driver_id, res.p3_driver_id,
               d1.name as p1_name, d2.name as p2_name, d3.name as p3_name
        FROM races r
        LEFT JOIN results res ON r.id = res.race_id
        LEFT JOIN drivers d1 ON res.p1_driver_id = d1.id
        LEFT JOIN drivers d2 ON res.p2_driver_id = d2.id
        LEFT JOIN drivers d3 ON res.p3_driver_id = d3.id
        ORDER BY r.round
    ''').fetchall()

    return [enrich_race_with_status(dict(r), r['p1_driver_id'] is not None) for r in races]

def get_next_open_race(db):
    """Get the next race that is open for predictions (future, no results)."""
    races = get_races_with_computed_status(db)
    for r in races:
        if r['status'] == 'open':
            return r
    return None


def _current_or_next_round(db):
    """Resolve "current round forward" for a new league's default scoring window.

    Prefers the next open race (same logic as get_next_open_race). If none is
    open (e.g. a race is locked/in-progress with nothing else open yet), fall
    back to the next non-completed race so a league created mid-weekend still
    starts at a sensible round rather than silently reverting to null/whole
    season.
    """
    races = get_races_with_computed_status(db)
    for r in races:
        if r['status'] == 'open':
            return r['round']
    for r in races:
        if r['status'] != 'completed':
            return r['round']
    return races[-1]['round'] if races else None


def create_league(db, admin_user_id, name, emoji_or_color, whole_season=False, start_round=None):
    """Create a league and immediately seat the creator as its admin member.

    No predictions/scores rows are touched here - a league is purely a view
    over the existing global game (E3).
    """
    name = (name or '').strip()
    emoji_or_color = (emoji_or_color or '').strip()
    if not name:
        raise ValueError('League name is required')
    if not emoji_or_color:
        raise ValueError('Emoji or color is required')

    current_round = _current_or_next_round(db)

    if whole_season:
        resolved_start_round = None
    elif start_round is not None:
        resolved_start_round = start_round
    else:
        resolved_start_round = current_round

    cur = db.execute(
        '''INSERT INTO leagues (name, emoji_or_color, start_round, whole_season, admin_user_id)
           VALUES (?, ?, ?, ?, ?)''',
        (name, emoji_or_color, resolved_start_round, 1 if whole_season else 0, admin_user_id)
    )
    league_id = cur.lastrowid

    db.execute(
        '''INSERT INTO league_members (league_id, user_id, joined_at_round, is_admin)
           VALUES (?, ?, ?, 1)''',
        (league_id, admin_user_id, current_round)
    )
    db.commit()
    return league_id


def is_league_member(db, league_id, user_id):
    """Membership lookup - true immediately for the creating admin, no separate invite needed."""
    row = db.execute(
        'SELECT 1 FROM league_members WHERE league_id = ? AND user_id = ?',
        (league_id, user_id)
    ).fetchone()
    return row is not None


def get_league_members(db, league_id):
    return db.execute(
        'SELECT * FROM league_members WHERE league_id = ? ORDER BY joined_at',
        (league_id,)
    ).fetchall()


def get_user_leagues(db, user_id):
    """Leagues a user belongs to, most recently created first."""
    return db.execute(
        '''SELECT l.* FROM leagues l
           JOIN league_members lm ON lm.league_id = l.id
           WHERE lm.user_id = ?
           ORDER BY l.created_at DESC''',
        (user_id,)
    ).fetchall()

# --- Routes ---

@app.route('/')
def index():
    """Landing page - redirect to home if logged in, else show username form."""
    user = get_current_user()
    if user:
        return redirect(url_for('home'))
    return render_template('index.html')

@app.route('/set-username', methods=['POST'])
def set_username():
    """Set username and create session."""
    username = request.form.get('username', '').strip()
    if not username:
        flash('Please enter a username', 'error')
        return redirect(url_for('index'))

    db = get_db()
    
    # Check if user with this username already exists
    existing = db.execute(
        'SELECT session_id FROM users WHERE username = ?',
        (username,)
    ).fetchone()
    
    if existing:
        # Reuse existing user's session_id
        session_id = existing['session_id']
    else:
        # Create new user with new session_id; new anonymous users are not
        # flagged as legacy (they signed up after email auth was available).
        session_id = str(uuid.uuid4())
        db.execute(
            'INSERT INTO users (session_id, username, is_synthetic, legacy_user) VALUES (?, ?, 0, ?)',
            (session_id, username, 0)
        )
        db.commit()

    session['session_id'] = session_id
    session.permanent = True
    return redirect(url_for('home'))

@app.route('/home')
def home():
    """Home page showing upcoming race and user's predictions."""
    user = get_current_user()
    if not user:
        return redirect(url_for('index'))

    db = get_db()
    next_race = get_next_open_race(db)
    if not next_race:
        races = get_races_with_computed_status(db)
        next_race = races[0] if races else None
        if next_race and next_race['status'] != 'open':
            next_race = next((r for r in races if r['status'] in ('open', 'locked')), next_race)

    user_prediction = None
    if next_race:
        try:
            user_prediction = db.execute('''
                SELECT p.*, d1.name as p1_name, d2.name as p2_name, d3.name as p3_name
                FROM predictions p
                JOIN drivers d1 ON p.p1_driver_id = d1.id
                JOIN drivers d2 ON p.p2_driver_id = d2.id
                JOIN drivers d3 ON p.p3_driver_id = d3.id
                WHERE p.user_id = ? AND p.race_id = ?
            ''', (user['session_id'], next_race['id'])).fetchone()
        except Exception:
            user_prediction = None

    total_score = db.execute(
        'SELECT COALESCE(SUM(points), 0) as total FROM scores WHERE user_id = ?',
        (user['session_id'],)
    ).fetchone()['total']

    has_pending = has_races_pending_results(db)

    return render_template('home.html',
                          user=user,
                          next_race=next_race,
                          user_prediction=user_prediction,
                          total_score=total_score,
                          has_pending_results=has_pending)

@app.route('/predict/<int:race_id>', methods=['GET', 'POST'])
def predict(race_id):
    """Make or update prediction for a race. Rejects if race has started."""
    user = get_current_user()
    if not user:
        return redirect(url_for('index'))

    db = get_db()
    race_row = db.execute('SELECT * FROM races WHERE id = ?', (race_id,)).fetchone()
    if not race_row:
        flash('Race not found', 'error')
        return redirect(url_for('home'))

    has_results = db.execute('SELECT 1 FROM results WHERE race_id = ?', (race_id,)).fetchone() is not None
    race = enrich_race_with_status(dict(race_row), has_results)

    if race['status'] == 'locked':
        flash('Predictions are locked - the race has started.', 'error')
        return redirect(url_for('home'))

    if race['status'] == 'completed':
        flash('This race has already been completed.', 'error')
        return redirect(url_for('home'))

    drivers = db.execute('SELECT * FROM drivers ORDER BY name').fetchall()

    if request.method == 'POST':
        p1 = request.form.get('p1')
        p2 = request.form.get('p2')
        p3 = request.form.get('p3')

        if not all([p1, p2, p3]):
            flash('Please select all three positions', 'error')
            return redirect(url_for('predict', race_id=race_id))

        if len(set([p1, p2, p3])) != 3:
            flash('Please select three different drivers', 'error')
            return redirect(url_for('predict', race_id=race_id))

        # ADM-006/ADM-007: Check for existing prediction before inserting
        existing_check = db.execute(
            'SELECT 1 FROM predictions WHERE user_id = ? AND race_id = ?',
            (user['session_id'], race_id)
        ).fetchone()
        if existing_check:
            flash('You already submitted predictions for this race. Visit the race page to update.', 'error')
            return redirect(url_for('races'))

        try:
            db.execute('''
                INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
                VALUES (?, ?, ?, ?, ?)
            ''', (user['session_id'], race_id, p1, p2, p3))
            db.commit()
            flash('Prediction saved!', 'success')
            return redirect(url_for('home'))
        except Exception as e:
            db.rollback()
            # ADM-006/ADM-007: Graceful fallback for any remaining constraint violations
            flash('You already submitted predictions for this race. Visit the race page to update.', 'error')
            return redirect(url_for('races'))

    existing = db.execute('''
        SELECT p1_driver_id, p2_driver_id, p3_driver_id
        FROM predictions WHERE user_id = ? AND race_id = ?
    ''', (user['session_id'], race_id)).fetchone()

    return render_template('predict.html', race=race, drivers=drivers, existing=existing)

@app.route('/leaderboard')
def leaderboard():
    """Show leaderboard with all users and their scores.

    Query params:
        season: Filter by season year (e.g. ?season=2026).
                If 'current' or omitted, defaults to current year.
    """
    user = get_current_user()
    if not user:
        return redirect(url_for('index'))

    db = get_db()

    # Determine season filter
    current_year = datetime.now().year
    season_param = request.args.get('season', 'current')

    if season_param == 'current' or season_param == '':
        filter_year = current_year
    else:
        try:
            filter_year = int(season_param)
        except ValueError:
            filter_year = current_year

    # Build race filter for SQL
    race_filter_sql = "AND strftime('%Y', r.date) = ?"
    race_filter_args = (str(filter_year),)

    # Get users with scores filtered by season
    # F1-111: exclude synthetic replay/persona users from public leaderboards.
    users = db.execute(f'''
        SELECT u.*, COALESCE(SUM(s.points), 0) as total_score
        FROM users u
        LEFT JOIN scores s ON u.session_id = s.user_id
        LEFT JOIN races r ON s.race_id = r.id
        WHERE u.is_synthetic = 0
        GROUP BY u.session_id
        HAVING COUNT(CASE WHEN r.id IS NOT NULL THEN 1 END) = 0
           OR SUM(CASE WHEN strftime('%Y', r.date) = ? THEN s.points ELSE 0 END) >= 0
        ORDER BY total_score DESC
    ''', (str(filter_year),)).fetchall()

    # Re-query properly: users and their scores from races in the selected season
    users = db.execute(f'''
        SELECT u.*, COALESCE(SUM(s.points), 0) as total_score
        FROM users u
        LEFT JOIN scores s ON u.session_id = s.user_id
        LEFT JOIN races r ON s.race_id = r.id AND strftime('%Y', r.date) = ?
        WHERE u.is_synthetic = 0
        GROUP BY u.session_id
        ORDER BY total_score DESC
    ''', (str(filter_year),)).fetchall()

    # Get races in the selected season
    races = db.execute('''
        SELECT r.*
        FROM races r
        WHERE r.status = 'completed'
        AND strftime('%Y', r.date) = ?
        ORDER BY r.date ASC
    ''', (str(filter_year),)).fetchall()

    score_matrix = {}
    for u in users:
        score_matrix[u['session_id']] = {}
        for race in races:
            score = db.execute(
                'SELECT points FROM scores WHERE user_id = ? AND race_id = ?',
                (u['session_id'], race['id'])
            ).fetchone()
            score_matrix[u['session_id']][race['id']] = score['points'] if score else '-'

    return render_template('leaderboard.html',
                          users=users,
                          races=races,
                          score_matrix=score_matrix,
                          current_user=user,
                          season=filter_year)

@app.route('/live')
def live():
    """
    Live leaderboard page showing all races and aggregate standings.
    BUD-77: Implement /live leaderboard page.
    - Shows all races for current season with lock/live status
    - Live races show countdown timer to lock
    - Locked races show final results
    - User's predictions shown alongside each race
    - Points calculated and displayed per race
    - Aggregate leaderboard across all races
    - Auto-refresh every 60 seconds
    """
    user = get_current_user()
    if not user:
        return redirect(url_for('index'))

    db = get_db()
    season = app.config['F1_SEASON']

    # Get all races with computed status
    all_races = get_races_with_computed_status(db)

    # Filter to current season and add slug
    races = []
    for r in all_races:
        if str(r['date'].year) == str(season):
            r['slug'] = race_slug(r)
            races.append(r)

    # Get current user's predictions for all races
    predictions = {}
    for race in races:
        pred = db.execute('''
            SELECT p.*, d1.name as p1_name, d1.driver_id as p1_api_id,
                   d2.name as p2_name, d2.driver_id as p2_api_id,
                   d3.name as p3_name, d3.driver_id as p3_api_id
            FROM predictions p
            JOIN drivers d1 ON p.p1_driver_id = d1.id
            JOIN drivers d2 ON p.p2_driver_id = d2.id
            JOIN drivers d3 ON p.p3_driver_id = d3.id
            WHERE p.user_id = ? AND p.race_id = ?
        ''', (user['session_id'], race['id'])).fetchone()
        predictions[race['id']] = dict(pred) if pred else None

    # Get race results for completed races
    race_results = {}
    for race in races:
        if race['status'] == 'completed':
            result = db.execute('''
                SELECT d1.name as p1_name, d2.name as p2_name, d3.name as p3_name
                FROM results r
                JOIN drivers d1 ON r.p1_driver_id = d1.id
                JOIN drivers d2 ON r.p2_driver_id = d2.id
                JOIN drivers d3 ON r.p3_driver_id = d3.id
                WHERE r.race_id = ?
            ''', (race['id'],)).fetchone()
            race_results[race['id']] = dict(result) if result else None

    # Calculate user's points per race
    user_points_per_race = {}
    for race in races:
        if race['status'] == 'completed':
            score = db.execute(
                'SELECT points FROM scores WHERE user_id = ? AND race_id = ?',
                (user['session_id'], race['id'])
            ).fetchone()
            user_points_per_race[race['id']] = score['points'] if score else 0

    # Calculate aggregate leaderboard
    # F1-111: exclude synthetic replay/persona users from live leaderboards.
    leaderboard = db.execute('''
        SELECT u.session_id, u.username,
               COALESCE(SUM(s.points), 0) as total_score
        FROM users u
        LEFT JOIN scores s ON u.session_id = s.user_id
        LEFT JOIN races r ON s.race_id = r.id AND strftime('%Y', r.date) = ?
        WHERE u.is_synthetic = 0
        GROUP BY u.session_id
        ORDER BY total_score DESC
    ''', (str(season),)).fetchall()

    # Calculate user's total score
    user_total = 0
    for row in leaderboard:
        if row['session_id'] == user['session_id']:
            user_total = row['total_score']
            break

    return render_template('live.html',
                          races=races,
                          predictions=predictions,
                          race_results=race_results,
                          user_points_per_race=user_points_per_race,
                          leaderboard=leaderboard,
                          user_total=user_total,
                          current_user=user,
                          season=season,
                          refresh_interval=60)

@app.route('/leagues')
def leagues():
    """List the current user's leagues."""
    user = get_current_user()
    if not user:
        return redirect(url_for('index'))

    db = get_db()
    user_leagues = get_user_leagues(db, user['session_id'])
    return render_template('leagues.html', leagues=user_leagues)


@app.route('/leagues/new', methods=['GET'])
def new_league():
    """Show the create-league form."""
    user = get_current_user()
    if not user:
        flash('Please sign in to create a league', 'error')
        return redirect(url_for('index'))
    return render_template('league_new.html')


@app.route('/leagues', methods=['POST'])
def create_league_route():
    """Create a league (name + emoji/color required; scoring window optional)."""
    user = get_current_user()
    if not user:
        flash('Please sign in to create a league', 'error')
        return redirect(url_for('index'))

    name = request.form.get('name', '').strip()
    emoji_or_color = request.form.get('emoji_or_color', '').strip()
    window = request.form.get('window', 'current')  # 'current' (default) | 'whole_season'

    if not name or not emoji_or_color:
        flash('League name and emoji/color are required', 'error')
        return redirect(url_for('new_league'))

    db = get_db()
    league_id = create_league(
        db, user['session_id'], name, emoji_or_color,
        whole_season=(window == 'whole_season'),
    )
    flash('League created!', 'success')
    return redirect(url_for('league_detail', league_id=league_id))


@app.route('/leagues/<int:league_id>')
def league_detail(league_id):
    """League detail: name, emoji/color, scoring window, members."""
    user = get_current_user()
    if not user:
        return redirect(url_for('index'))

    db = get_db()
    league = db.execute('SELECT * FROM leagues WHERE id = ?', (league_id,)).fetchone()
    if not league:
        flash('League not found', 'error')
        return redirect(url_for('leagues'))

    members = get_league_members(db, league_id)
    return render_template('league_detail.html',
                          league=league,
                          members=members,
                          is_member=is_league_member(db, league_id, user['session_id']))


@app.route('/races')
def races():
    """Show all races and their status."""
    user = get_current_user()
    if not user:
        return redirect(url_for('index'))

    db = get_db()
    all_races = get_races_with_computed_status(db)

    predictions = {}
    for race in all_races:
        pred = db.execute('''
            SELECT p.*, d1.name as p1_name, d2.name as p2_name, d3.name as p3_name
            FROM predictions p
            JOIN drivers d1 ON p.p1_driver_id = d1.id
            JOIN drivers d2 ON p.p2_driver_id = d2.id
            JOIN drivers d3 ON p.p3_driver_id = d3.id
            WHERE p.user_id = ? AND p.race_id = ?
        ''', (user['session_id'], race['id'])).fetchone()
        if pred:
            predictions[race['id']] = pred

    has_pending = has_races_pending_results(db)
    for r in all_races:
        r['slug'] = race_slug(r)

    return render_template('races.html',
                          races=all_races,
                          predictions=predictions,
                          has_pending_results=has_pending,
                          is_admin=is_admin(user))


def _race_detail_impl(race_id, db, user):
    """Shared logic for race detail. Returns (race, has_results, result_data, predictions, scores_by_user) or None."""
    race_row = db.execute('SELECT * FROM races WHERE id = ?', (race_id,)).fetchone()
    if not race_row:
        return None

    has_results = db.execute('SELECT 1 FROM results WHERE race_id = ?', (race_id,)).fetchone() is not None
    race = enrich_race_with_status(dict(race_row), has_results)

    if race['status'] not in ('locked', 'completed'):
        return None

    result_names = None
    result_ids = None
    if has_results:
        res = db.execute('''
            SELECT res.p1_driver_id, res.p2_driver_id, res.p3_driver_id,
                   d1.name as p1_name, d2.name as p2_name, d3.name as p3_name
            FROM results res
            JOIN drivers d1 ON res.p1_driver_id = d1.id
            JOIN drivers d2 ON res.p2_driver_id = d2.id
            JOIN drivers d3 ON res.p3_driver_id = d3.id
            WHERE res.race_id = ?
        ''', (race_id,)).fetchone()
        if res:
            result_names = {'p1_name': res['p1_name'], 'p2_name': res['p2_name'], 'p3_name': res['p3_name']}
            result_ids = {'p1_driver_id': res['p1_driver_id'], 'p2_driver_id': res['p2_driver_id'], 'p3_driver_id': res['p3_driver_id']}

    predictions = db.execute('''
        SELECT p.*, u.username,
               d1.name as p1_name, d2.name as p2_name, d3.name as p3_name
        FROM predictions p
        JOIN users u ON p.user_id = u.session_id
        JOIN drivers d1 ON p.p1_driver_id = d1.id
        JOIN drivers d2 ON p.p2_driver_id = d2.id
        JOIN drivers d3 ON p.p3_driver_id = d3.id
        WHERE p.race_id = ? AND u.is_synthetic = 0
        ORDER BY u.username
    ''', (race_id,)).fetchall()

    scores_by_user = {}
    if has_results:
        for row in db.execute('SELECT user_id, points FROM scores WHERE race_id = ?', (race_id,)).fetchall():
            scores_by_user[row['user_id']] = row['points']

    return (race, has_results, result_names, result_ids, predictions, scores_by_user)


@app.route('/race/<int:race_id>')
def race_detail_by_id(race_id):
    """Redirect to slug URL for clean address bar."""
    user = get_current_user()
    if not user:
        return redirect(url_for('index'))
    db = get_db()
    race_row = db.execute('SELECT * FROM races WHERE id = ?', (race_id,)).fetchone()
    if not race_row:
        flash('Race not found', 'error')
        return redirect(url_for('races'))
    race = enrich_race_with_status(dict(race_row), db.execute('SELECT 1 FROM results WHERE race_id = ?', (race_id,)).fetchone() is not None)
    if race['status'] not in ('locked', 'completed'):
        flash('Picks are visible after the race is locked', 'error')
        return redirect(url_for('races'))
    return redirect(url_for('race_detail', slug=race_slug(race)), code=301)


@app.route('/race/<slug>')
def race_detail(slug):
    """Show individual race with all voters' picks. URL uses slug e.g. 2026_chinese."""
    user = get_current_user()
    if not user:
        return redirect(url_for('index'))

    db = get_db()
    all_races = get_races_with_computed_status(db)
    race = None
    for r in all_races:
        if race_slug(r) == slug:
            race = r
            break
    if not race:
        flash('Race not found', 'error')
        return redirect(url_for('races'))

    data = _race_detail_impl(race['id'], db, user)
    if not data:
        flash('Picks are visible after the race is locked', 'error')
        return redirect(url_for('races'))

    race, has_results, result_names, result_ids, predictions, scores_by_user = data

    return render_template('race_detail.html',
                          race=race,
                          race_slug=race_slug(race),
                          predictions=predictions,
                          result_names=result_names,
                          result_ids=result_ids or {},
                          scores_by_user=scores_by_user)


@app.route('/race/<int:race_id>/live')
def live_leaderboard(race_id):
    """
    Live race leaderboard page.
    LL-001: /race/<id>/live page accessible during race.
    LL-002: Auto-refresh every 30 seconds (handled in template).
    LL-003: Projected points calculation.
    LL-004: Position change highlights.
    LL-005: Best/worst case projection.
    LL-006: Driver tracker.
    LL-007: Rate limiting (via caching).
    LL-008: Graceful fallback if API unavailable.
    """
    user = get_current_user()
    if not user:
        return redirect(url_for('index'))
    
    db = get_db()
    
    # Get race info
    race_row = db.execute('SELECT * FROM races WHERE id = ?', (race_id,)).fetchone()
    if not race_row:
        flash('Race not found', 'error')
        return redirect(url_for('races'))
    
    has_results = db.execute('SELECT 1 FROM results WHERE race_id = ?', (race_id,)).fetchone() is not None
    race = enrich_race_with_status(dict(race_row), has_results)
    
    # Only allow live view for locked races (during race)
    if race['status'] == 'completed':
        # If race is completed, redirect to race detail
        return redirect(url_for('race_detail', slug=race_slug(race)))
    
    if race['status'] == 'open':
        flash('Race has not started yet', 'error')
        return redirect(url_for('races'))
    
    # Check if we have cached live data (rate limiting)
    cached_positions = _get_cached_live_data(race_id)
    
    if cached_positions is not None:
        live_positions = cached_positions
        api_available = True
        last_updated = _live_data_cache[race_id]['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
    else:
        # Fetch fresh live data from API
        live_positions = fetch_live_race_data(db, race)

        if live_positions:
            _set_cached_live_data(race_id, live_positions)
            api_available = True
            # F1-02: when OpenF1 itself is down, fetch_live_race_data falls
            # back to the last-known-good cache rather than raising — report
            # the cached payload's real age instead of claiming "now".
            if live_positions.from_cache:
                last_updated = live_positions.fetched_at.strftime('%Y-%m-%d %H:%M:%S')
            else:
                last_updated = _now_utc().strftime('%Y-%m-%d %H:%M:%S')
        else:
            # LL-008: Graceful fallback - use empty/placeholder data
            api_available = False
            live_positions = []
            last_updated = None

    # F1-02 data-age indicator: surfaced whenever positions came from the
    # last-known-good cache (OpenF1 was unreachable), not just when stale.
    data_from_cache = getattr(live_positions, 'from_cache', False)
    data_stale = getattr(live_positions, 'is_stale', False)
    data_age_label = getattr(live_positions, 'age_label', None)
    
    # Get all predictions for this race with projected points
    # F1-111: exclude synthetic replay/persona users from live race leaderboards.
    predictions = db.execute('''
        SELECT p.*, u.username,
               d1.name as p1_name, d1.driver_id as p1_api_id,
               d2.name as p2_name, d2.driver_id as p2_api_id,
               d3.name as p3_name, d3.driver_id as p3_api_id
        FROM predictions p
        JOIN users u ON p.user_id = u.session_id
        JOIN drivers d1 ON p.p1_driver_id = d1.id
        JOIN drivers d2 ON p.p2_driver_id = d2.id
        JOIN drivers d3 ON p.p3_driver_id = d3.id
        WHERE p.race_id = ? AND u.is_synthetic = 0
        ORDER BY u.username
    ''', (race_id,)).fetchall()
    
    # Calculate projected points for each user
    user_projections = []
    for pred in predictions:
        projected = calculate_projected_points(pred, live_positions)
        best_worst = calculate_best_worst_case(pred, live_positions)
        user_projections.append({
            'username': pred['username'],
            'user_id': pred['user_id'],
            'prediction': pred,
            'projected_points': projected,
            'best_case': best_worst['best'],
            'worst_case': best_worst['worst'],
            'current_points': best_worst['current'],
        })
    
    # Sort by projected points descending
    user_projections.sort(key=lambda x: x['projected_points']['projected_points'], reverse=True)
    
    # Get current user's projection for highlighting
    current_user_projection = None
    for proj in user_projections:
        if proj['user_id'] == user['session_id']:
            current_user_projection = proj
            break
    
    # Get driver's current positions for tracking (LL-006)
    driver_track_status = {}
    if current_user_projection:
        pred = current_user_projection['prediction']
        for did_key, api_id_key, name_key in [
            ('p1', 'p1_api_id', 'p1_name'),
            ('p2', 'p2_api_id', 'p2_name'),
            ('p3', 'p3_api_id', 'p3_name'),
        ]:
            api_id = pred[api_id_key]
            driver_name = pred[name_key]
            
            # Find current position
            curr_pos = None
            for pos in live_positions:
                if pos['driver_id'] == api_id:
                    curr_pos = pos['position']
                    break
            
            driver_track_status[did_key] = {
                'name': driver_name,
                'current_position': curr_pos,
                'predicted_position': int(did_key[1]),  # p1 -> 1, p2 -> 2, p3 -> 3
            }
    
    return render_template(
        'live_leaderboard.html',
        race=race,
        race_slug=race_slug(race),
        live_positions=live_positions,
        user_projections=user_projections,
        current_user_projection=current_user_projection,
        driver_track_status=driver_track_status,
        api_available=api_available,
        last_updated=last_updated,
        refresh_interval=LIVE_REFRESH_INTERVAL_SEC,
        data_from_cache=data_from_cache,
        data_stale=data_stale,
        data_age_label=data_age_label,
    )


@app.route('/login', methods=['GET'])
def login():
    """Show email login form."""
    return render_template('login.html')


@app.route('/login/request', methods=['POST'])
def login_request():
    """Request a magic-link login email."""
    email = request.form.get('email', '').strip()
    if not email or '@' not in email:
        flash('Please enter a valid email address', 'error')
        return redirect(url_for('login'))

    create_login_token(email)
    return render_template('login_sent.html', email=_normalize_email(email))


@app.route('/login/verify/<token>')
def login_verify(token):
    """Consume a magic-link token and log the user in."""
    email = consume_login_token(token)
    if not email:
        flash('That login link is invalid or has expired.', 'error')
        return redirect(url_for('login'))

    bind_email_to_session(email)
    flash('You are now logged in.', 'success')
    return redirect(url_for('home'))


@app.route('/login/oauth/google')
def login_oauth_google():
    """Initiate Google OAuth sign-in."""
    client_id = app.config.get('GOOGLE_OAUTH_CLIENT_ID', '')
    if not client_id:
        flash('Google sign-in is not configured.', 'error')
        return redirect(url_for('login'))

    state = _generate_oauth_state()
    session['oauth_state'] = state
    return redirect(_google_auth_url(state))


@app.route('/login/oauth/google/callback')
def login_oauth_google_callback():
    """Handle the Google OAuth callback and log the user in."""
    stored_state = session.pop('oauth_state', None)
    returned_state = request.args.get('state')
    if not stored_state or not returned_state or stored_state != returned_state:
        flash('Invalid OAuth state. Please try again.', 'error')
        return redirect(url_for('login'))

    code = request.args.get('code')
    if not code:
        flash('Google sign-in was cancelled or failed.', 'error')
        return redirect(url_for('login'))

    token_response = _exchange_google_code(code)
    if not token_response:
        flash('Unable to verify Google sign-in.', 'error')
        return redirect(url_for('login'))

    access_token = token_response.get('access_token')
    if not access_token:
        flash('Unable to verify Google sign-in.', 'error')
        return redirect(url_for('login'))

    userinfo = _fetch_google_userinfo(access_token)
    if not userinfo or not userinfo.get('email'):
        flash('Unable to retrieve your email from Google.', 'error')
        return redirect(url_for('login'))

    email = userinfo['email']
    if not userinfo.get('email_verified'):
        flash('Please use a Google account with a verified email.', 'error')
        return redirect(url_for('login'))

    bind_email_to_session(email)
    session.permanent = True
    flash('You are now logged in with Google.', 'success')
    return redirect(url_for('home'))


@app.route('/logout')
def logout():
    """Clear session."""
    session.clear()
    return redirect(url_for('index'))


@app.route('/debug/magic-link/<email>')
def debug_magic_link(email):
    """Debug endpoint: return the latest unused magic link for an email.

    Only available in dev/test environments so tests can retrieve tokens
    without parsing stdout.
    """
    if app.config.get('ENVIRONMENT') != 'dev' and not os.environ.get('TESTING'):
        return jsonify({'error': 'Not available'}), 404

    db = get_db()
    normalized = _normalize_email(email)
    now = _now_utc().strftime('%Y-%m-%d %H:%M:%S')
    row = db.execute('''
        SELECT token, expires_at FROM login_tokens
        WHERE email = ? AND used = 0 AND expires_at > ?
        ORDER BY created_at DESC LIMIT 1
    ''', (normalized, now)).fetchone()

    if not row:
        return jsonify({'token': None, 'link': None})

    token = row['token']
    return jsonify({
        'token': token,
        'link': _login_token_url(token),
        'expires_at': row['expires_at'],
    })


@app.route('/migrate', methods=['GET'])
def migrate():
    """Show legacy-user migration prompt.

    Legacy users (existing anonymous users created before email auth) land
    here from the banner or direct link to add an email and keep their
    prediction history.
    """
    user = get_current_user()
    if not user:
        return redirect(url_for('index'))
    if user['email']:
        flash('Your account already has an email.', 'info')
        return redirect(url_for('home'))
    return render_template('migrate.html', user=user)


@app.route('/migrate/send', methods=['POST'])
def migrate_send():
    """Create a magic link for a legacy user migrating to email login.

    Refuses emails already bound to another account so the current
    session's predictions are not orphaned by bind_email_to_session.
    """
    user = get_current_user()
    if not user:
        return redirect(url_for('index'))
    if user['email']:
        flash('Your account already has an email.', 'info')
        return redirect(url_for('home'))

    email = request.form.get('email', '').strip()
    normalized = _normalize_email(email)
    if not normalized or '@' not in normalized:
        flash('Please enter a valid email address', 'error')
        return redirect(url_for('migrate'))

    db = get_db()
    existing = _find_user_by_email(db, normalized)
    if existing and existing['session_id'] != user['session_id']:
        flash(
            'That email is already linked to another account. '
            'Log out and use Log in with email, or choose a different email.',
            'error'
        )
        return redirect(url_for('migrate'))

    create_login_token(email)
    return render_template('migrate_sent.html', email=normalized)


@app.route('/admin/legacy-users', methods=['GET', 'POST'])
@admin_required
def admin_legacy_users():
    """List legacy users and allow admins to trigger migration emails."""
    db = get_db()

    if request.method == 'POST':
        email = request.form.get('email', '').strip()
        normalized = _normalize_email(email)
        if normalized and '@' in normalized:
            create_login_token(email)
            flash(f'Migration link created for {normalized}.', 'success')
        else:
            flash('Please enter a valid email address', 'error')
        return redirect(url_for('admin_legacy_users'))

    users = db.execute('''
        SELECT session_id, username, email, created_at
        FROM users
        WHERE (email IS NULL OR email = '') AND legacy_user = 1
        ORDER BY created_at ASC
    ''').fetchall()

    return render_template('admin_legacy_users.html', users=users)


# Admin routes
@app.route('/admin/enter-results/<int:race_id>', methods=['GET', 'POST'])
@admin_required
def enter_results(race_id):
    """Enter actual race results manually (admin). API polling may have already updated."""
    db = get_db()

    race_row = db.execute('SELECT * FROM races WHERE id = ?', (race_id,)).fetchone()
    if not race_row:
        flash('Race not found', 'error')
        return redirect(url_for('races'))

    has_results = db.execute('SELECT 1 FROM results WHERE race_id = ?', (race_id,)).fetchone() is not None
    race = enrich_race_with_status(dict(race_row), has_results)
    drivers = db.execute('SELECT * FROM drivers ORDER BY name').fetchall()

    if request.method == 'POST':
        p1 = request.form.get('p1')
        p2 = request.form.get('p2')
        p3 = request.form.get('p3')

        if not all([p1, p2, p3]):
            flash('Please select all three positions', 'error')
            return redirect(url_for('enter_results', race_id=race_id))

        # Convert to integers for proper score calculation
        p1_int, p2_int, p3_int = int(p1), int(p2), int(p3)

        db.execute('''
            INSERT INTO results (race_id, p1_driver_id, p2_driver_id, p3_driver_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(race_id) DO UPDATE SET
                p1_driver_id = excluded.p1_driver_id,
                p2_driver_id = excluded.p2_driver_id,
                p3_driver_id = excluded.p3_driver_id
        ''', (race_id, p1_int, p2_int, p3_int))

        predictions = db.execute('SELECT * FROM predictions WHERE race_id = ?', (race_id,)).fetchall()
        result = {'p1_driver_id': p1_int, 'p2_driver_id': p2_int, 'p3_driver_id': p3_int}

        for pred in predictions:
            points = calculate_score(pred, result)
            db.execute('''
                INSERT INTO scores (user_id, race_id, points)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id, race_id) DO UPDATE SET points = excluded.points
            ''', (pred['user_id'], race_id, points))

        db.commit()
        flash('Results entered and scores calculated!', 'success')
        return redirect(url_for('races'))

    return render_template('enter_results.html', race=race, drivers=drivers)

@app.route('/admin/lock-race/<int:race_id>')
@admin_required
def lock_race(race_id):
    """Lock predictions for a race."""
    db = get_db()
    db.execute('UPDATE races SET status = ? WHERE id = ?', ('locked', race_id))
    db.commit()
    flash('Race predictions locked', 'success')
    return redirect(url_for('races'))


@app.route('/admin/delete-predictions', methods=['GET', 'POST'])
@admin_required
def delete_predictions():
    """
    Delete predictions for users matching a pattern that did NOT predict
    the given driver as P1. Used to remove duplicate/wrong 'brett' entries.
    """
    if request.method == 'GET':
        # Driver list drives the P1 picker, so the admin can't typo a name that
        # doesn't exist (which previously just flashed an error and bounced).
        drivers = get_db().execute('SELECT id, name, number, team FROM drivers ORDER BY number').fetchall()
        return render_template('admin_delete_predictions.html', drivers=drivers)

    username_pattern = (request.form.get('username_pattern') or request.args.get('username_pattern') or 'brett').strip()
    keep_p1_name = (request.form.get('keep_p1_name') or request.args.get('keep_p1_name') or 'Kimi').strip()
    if not username_pattern or not keep_p1_name:
        flash('username_pattern and keep_p1_name are required', 'error')
        return redirect(url_for('delete_predictions'))

    db = get_db()
    # Resolve driver id for keep_p1 (e.g. Kimi -> Kimi Antonelli)
    driver = db.execute(
        'SELECT id FROM drivers WHERE name LIKE ?', (f'%{keep_p1_name}%',)
    ).fetchone()
    if not driver:
        flash(f'No driver found matching "{keep_p1_name}"', 'error')
        return redirect(url_for('delete_predictions'))

    keep_p1_id = driver['id']
    pattern = f'%{username_pattern}%'
    session_ids = [
        row[0] for row in
        db.execute('SELECT session_id FROM users WHERE username LIKE ?', (pattern,)).fetchall()
    ]
    if not session_ids:
        flash(f'No users found matching username "{username_pattern}"', 'error')
        return redirect(url_for('delete_predictions'))

    # Predictions to remove: those users, and p1_driver_id != keep_p1_id
    placeholders = ','.join('?' * len(session_ids))
    to_delete = db.execute(
        f'''
        SELECT user_id, race_id FROM predictions
        WHERE user_id IN ({placeholders}) AND p1_driver_id != ?
        ''',
        (*session_ids, keep_p1_id)
    ).fetchall()

    for row in to_delete:
        db.execute('DELETE FROM scores WHERE user_id = ? AND race_id = ?', (row['user_id'], row['race_id']))
        db.execute('DELETE FROM predictions WHERE user_id = ? AND race_id = ?', (row['user_id'], row['race_id']))

    db.commit()
    n = len(to_delete)
    flash(f'Deleted {n} prediction(s) (and their scores) for users matching "{username_pattern}" that did not predict {keep_p1_name} as P1.', 'success')
    return redirect(url_for('races'))

# Driver refresh endpoint for CronJob
@app.route('/admin/refresh-drivers', methods=['POST'])
def refresh_drivers():
    """Refresh drivers from API - called by CronJob."""
    auth_header = request.headers.get('Authorization', '')
    expected = f"Bearer {app.config['DRIVER_REFRESH_SECRET']}"

    if auth_header != expected and app.config['DRIVER_REFRESH_SECRET']:
        return jsonify({'error': 'Unauthorized'}), 401

    db = get_db()
    success, message = refresh_drivers_from_api(db)

    if success:
        return jsonify({'status': 'success', 'message': message}), 200
    return jsonify({'status': 'error', 'message': message}), 500

@app.route('/admin/drivers-status')
def drivers_status():
    """Get driver refresh status."""
    db = get_db()

    count = db.execute('SELECT COUNT(*) FROM drivers').fetchone()[0]
    last_refresh = db.execute(
        'SELECT value FROM metadata WHERE key = "drivers_last_refresh"'
    ).fetchone()

    return jsonify({
        'driver_count': count,
        'last_refresh': last_refresh['value'] if last_refresh else None
    })

@app.route('/check-results', methods=['GET', 'POST'])
def check_results():
    """
    Check F1 API for race results and ingest. User-triggered with browser auto-retry.
    Retry param: 0-10, triggers auto-refresh when no results (max 10 retries = ~20 min).
    """
    user = get_current_user()
    if not user:
        return redirect(url_for('index'))

    db = get_db()
    retry = int(request.args.get('retry', 0))

    updated, _ = check_and_ingest_results(db)

    if updated:
        flash(f"Results updated for: {', '.join(updated)}", 'success')
        return redirect(url_for('leaderboard'))

    pending = get_races_pending_results(db)
    if not pending:
        return render_template('check_results.html', status='none', retry=retry)

    if retry >= MAX_RETRIES:
        return render_template(
            'check_results.html',
            status='exhausted',
            retry=retry,
            max_retries=MAX_RETRIES
        )

    return render_template(
        'check_results.html',
        status='retry',
        retry=retry,
        next_retry=retry + 1,
        max_retries=MAX_RETRIES,
        retry_interval_sec=RETRY_INTERVAL_SEC,
        pending_races=[r['name'] for r in pending]
    )

@app.route('/health')
def health():
    """
    Health check endpoint.

    Includes last-successful-fetch age so BUD-164's external observer can
    tell fetch failures are happening without shelling into the pod or
    needing DB access of its own — it just reads this endpoint.
    """
    db = get_db()
    last_ok = fetch_attempts.last_successful_fetch_at(db)
    payload = {
        'status': 'healthy',
        'last_successful_fetch_at': last_ok.isoformat() if last_ok else None,
        'last_successful_fetch_age_seconds': (
            round((datetime.now(timezone.utc) - last_ok).total_seconds()) if last_ok else None
        ),
    }
    return jsonify(payload)

@app.before_request
def before_request():
    """Auto-lock races that have started (all HTML/API except static and health)."""
    if request.endpoint in (None, 'static', 'health'):
        return
    # Include predict and admin routes so /predict/N cannot bypass lock after race start
    auto_lock_races()


# CLI commands
@app.cli.command('list-legacy-users')
def cli_list_legacy_users():
    """List users who still need email migration."""
    db = get_db()
    users = db.execute('''
        SELECT session_id, username, email, created_at
        FROM users
        WHERE (email IS NULL OR email = '') AND legacy_user = 1
        ORDER BY created_at ASC
    ''').fetchall()

    if not users:
        click.echo('No legacy users pending migration.')
        return

    click.echo(f'{len(users)} legacy user(s) pending migration:')
    for user in users:
        click.echo(
            f"  - {user['username']} "
            f"(session={user['session_id']}, created={user['created_at']})"
        )


@app.cli.command('send-migration-email')
@click.argument('email')
def cli_send_migration_email(email):
    """Create a magic link for EMAIL and log it (stubbed sender)."""
    normalized = _normalize_email(email)
    if not normalized or '@' not in normalized:
        raise click.BadParameter('Please provide a valid email address.')

    create_login_token(email)
    click.echo(f'Migration magic link created for {normalized}.')
    click.echo('Check application logs or /debug/magic-link for the link.')


# Initialize database on startup
with app.app_context():
    init_db()
    auto_lock_races()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
