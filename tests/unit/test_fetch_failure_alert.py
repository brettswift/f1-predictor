"""Unit tests for fetch-failure alerting (BUD-125 / F1-03)."""

import os
import sys
import sqlite3
import json
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

os.environ['DATABASE_PATH'] = ':memory:'
os.environ['F1_SEASON'] = '2026'
os.environ['OPENF1_OFFLINE'] = 'true'
os.environ['TELEGRAM_BOT_TOKEN'] = 'test-token'
os.environ['TELEGRAM_NOTIFY_CHAT_ID'] = '123456'

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'cron'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from alerting import (
    ensure_alert_tables,
    record_fetch_attempt,
    alert_if_needed,
    FetchOutcome,
    _is_actionable_failure,
    _race_started_long_ago,
    _active_alert_for_race,
)


def _db():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    return conn


def test_is_actionable_failure():
    """Only hard failures trigger alerts; transient not-ready does not."""
    assert _is_actionable_failure(FetchOutcome.API_ERROR) is True
    assert _is_actionable_failure(FetchOutcome.INCOMPLETE_PODIUM) is True
    assert _is_actionable_failure(FetchOutcome.UNRESOLVED_DRIVER) is True
    assert _is_actionable_failure(FetchOutcome.NO_RESULTS_YET) is False
    assert _is_actionable_failure(FetchOutcome.OK) is False


def test_race_started_long_ago():
    """4-hour threshold gates whether an empty result is treated as a failure."""
    now = datetime(2026, 6, 15, 20, 0, 0, tzinfo=timezone.utc)
    assert _race_started_long_ago('2026-06-15 14:00:00', now) is True
    assert _race_started_long_ago('2026-06-15 17:00:00', now) is False


def test_record_fetch_attempt_creates_tables():
    """Recording an attempt lazily creates the alert schema."""
    db = _db()
    record_fetch_attempt(db, 1, 'fetch_results', FetchOutcome.API_ERROR, error='boom')

    tables = {row[0] for row in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert 'fetch_attempts' in tables
    assert 'fetch_alerts' in tables

    row = db.execute('SELECT * FROM fetch_attempts').fetchone()
    assert row['race_id'] == 1
    assert row['outcome'] == FetchOutcome.API_ERROR


def test_alert_if_needed_sends_telegram_on_failure():
    """When a completed race has a hard failure, one Telegram alert is sent."""
    db = _db()
    now = datetime(2026, 6, 15, 20, 0, 0, tzinfo=timezone.utc)
    record_fetch_attempt(db, 1, 'fetch_results', FetchOutcome.API_ERROR, error='timeout')

    sent = []

    def fake_send(msg, *, bot_token=None, chat_id=None):
        sent.append(msg)
        return {'ok': True, 'result': {'message_id': 42}}

    with patch('alerting.send_telegram_alert', fake_send):
        alert_if_needed(db, 1, '2026-06-15 14:00:00', now=now)

    assert len(sent) == 1
    assert 'F1 results fetch failure' in sent[0]
    assert 'timeout' in sent[0]

    # A second call in the same failure cycle is suppressed.
    with patch('alerting.send_telegram_alert', fake_send):
        alert_if_needed(db, 1, '2026-06-15 14:00:00', now=now)

    assert len(sent) == 1


def test_alert_if_needed_resolves_after_success():
    """An OK attempt resolves the active alert so a future failure re-alerts."""
    db = _db()
    now = datetime(2026, 6, 15, 20, 0, 0, tzinfo=timezone.utc)

    record_fetch_attempt(db, 2, 'fetch_results', FetchOutcome.API_ERROR, error='timeout')
    with patch('alerting.send_telegram_alert', return_value={'ok': True, 'result': {'message_id': 1}}):
        alert_if_needed(db, 2, '2026-06-15 14:00:00', now=now)

    active_before = _active_alert_for_race(db, 2)
    assert active_before is not None

    record_fetch_attempt(db, 2, 'fetch_results', FetchOutcome.OK)
    alert_if_needed(db, 2, '2026-06-15 14:00:00', now=now)

    active_after = _active_alert_for_race(db, 2)
    assert active_after is None


def test_alert_if_needed_no_alert_before_threshold():
    """No alert is sent while the race is still within its expected window."""
    db = _db()
    now = datetime(2026, 6, 15, 17, 0, 0, tzinfo=timezone.utc)

    record_fetch_attempt(db, 3, 'fetch_results', FetchOutcome.API_ERROR, error='timeout')
    sent = []

    with patch('alerting.send_telegram_alert', lambda *a, **k: sent.append(a)):
        alert_if_needed(db, 3, '2026-06-15 14:00:00', now=now)

    assert len(sent) == 0


def test_alert_if_needed_disabled_without_env():
    """If Telegram env vars are missing, alerting does nothing."""
    db = _db()
    now = datetime(2026, 6, 15, 20, 0, 0, tzinfo=timezone.utc)

    record_fetch_attempt(db, 4, 'fetch_results', FetchOutcome.API_ERROR, error='timeout')
    sent = []

    with patch('alerting.send_telegram_alert', lambda *a, **k: sent.append(a)):
        with patch('alerting.alert_enabled', return_value=False):
            alert_if_needed(db, 4, '2026-06-15 14:00:00', now=now)

    assert len(sent) == 0


def test_no_results_yet_does_not_alert():
    """An empty-but-expected race that is just 'not finished yet' does not alert."""
    db = _db()
    now = datetime(2026, 6, 15, 20, 0, 0, tzinfo=timezone.utc)

    record_fetch_attempt(db, 5, 'fetch_results', FetchOutcome.NO_RESULTS_YET)
    sent = []

    with patch('alerting.send_telegram_alert', lambda *a, **k: sent.append(a)):
        alert_if_needed(db, 5, '2026-06-15 14:00:00', now=now)

    assert len(sent) == 0
