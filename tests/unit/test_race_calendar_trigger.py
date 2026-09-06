"""Unit tests for F1-114 (BUD-172): race-calendar trigger for the persona runner."""

from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import responses

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import openf1  # noqa: E402
import race_calendar_trigger as rct  # noqa: E402

BASE = openf1.OPENF1_BASE_URL


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    openf1.ensure_cache_table(conn)
    yield conn
    conn.close()


@pytest.fixture(autouse=True)
def no_retry_sleep(monkeypatch):
    monkeypatch.setattr(openf1.time, "sleep", lambda _s: None)


@pytest.fixture(autouse=True)
def network_mocked(monkeypatch):
    monkeypatch.setattr(openf1, "OFFLINE", False)


def _iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "+00:00")


def _sessions(now: datetime):
    """Three race sessions: one in the past, two upcoming, unordered on purpose."""
    return [
        {"session_key": 200, "session_type": "Race", "is_cancelled": False,
         "circuit_short_name": "Melbourne", "country_name": "Australia",
         "date_start": _iso(now + timedelta(days=7))},
        {"session_key": 100, "session_type": "Race", "is_cancelled": False,
         "circuit_short_name": "Sakhir", "country_name": "Bahrain",
         "date_start": _iso(now - timedelta(days=7))},  # already past lock
        {"session_key": 300, "session_type": "Race", "is_cancelled": False,
         "circuit_short_name": "Suzuka", "country_name": "Japan",
         "date_start": _iso(now + timedelta(days=14))},
    ]


class TestBuildCalendar:
    @responses.activate
    def test_builds_from_openf1_session_data_not_hardcoded(self, db):
        now = datetime.now(timezone.utc)
        responses.add(responses.GET, f"{BASE}/sessions", json=_sessions(now), status=200)

        calendar = rct.build_calendar(season=2026, db=db)

        assert [t.session_key for t in calendar] == [100, 200, 300]
        assert all(isinstance(t.lock_at, datetime) for t in calendar)

    @responses.activate
    def test_trigger_time_is_five_minutes_before_lock_by_default(self, db):
        now = datetime.now(timezone.utc)
        responses.add(responses.GET, f"{BASE}/sessions", json=_sessions(now), status=200)

        calendar = rct.build_calendar(season=2026, db=db)
        melbourne = next(t for t in calendar if t.session_key == 200)

        assert melbourne.trigger_at == melbourne.lock_at - timedelta(seconds=300)

    @responses.activate
    def test_missing_date_start_is_skipped(self, db):
        sessions = _sessions(datetime.now(timezone.utc))
        sessions.append({"session_key": 400, "session_type": "Race", "is_cancelled": False,
                          "circuit_short_name": "Unknown", "date_start": None})
        responses.add(responses.GET, f"{BASE}/sessions", json=sessions, status=200)

        calendar = rct.build_calendar(season=2026, db=db)

        assert 400 not in [t.session_key for t in calendar]


class TestRaceCalendarTrigger:
    @responses.activate
    def test_skip_past_lock_does_not_fire_callback(self, db):
        now = datetime.now(timezone.utc)
        responses.add(responses.GET, f"{BASE}/sessions", json=_sessions(now), status=200)

        fired = []
        trigger = rct.RaceCalendarTrigger(on_trigger=fired.append, season=2026, db=db)
        skipped = trigger.skip_past_lock(now)

        assert [t.session_key for t in skipped] == [100]
        assert fired == []
        assert 100 in trigger._fired

    @responses.activate
    def test_fast_forward_fires_all_remaining_triggers_in_rapid_succession(self, db):
        now = datetime.now(timezone.utc)
        responses.add(responses.GET, f"{BASE}/sessions", json=_sessions(now), status=200)

        fired = []
        trigger = rct.RaceCalendarTrigger(on_trigger=fired.append, season=2026, db=db)
        count = trigger.run_fast_forward()

        # Bahrain (100) was already past lock -> skipped, not fired.
        assert count == 2
        assert [t.session_key for t in fired] == [200, 300]

    @responses.activate
    def test_run_with_fast_forward_true_delegates_to_fast_forward(self, db):
        now = datetime.now(timezone.utc)
        responses.add(responses.GET, f"{BASE}/sessions", json=_sessions(now), status=200)

        fired = []
        trigger = rct.RaceCalendarTrigger(on_trigger=fired.append, season=2026, db=db)
        count = trigger.run(fast_forward=True)

        assert count == 2
        assert len(trigger._fired) == 3  # 2 fired + 1 skipped-past-lock

    def test_run_wall_clock_only_fires_due_triggers_within_bound(self):
        """Wall-clock mode: a trigger due in the far future must not fire
        within a bounded number of poll iterations."""
        now = datetime.now(timezone.utc)
        far_future_lock = now + timedelta(hours=1)
        calendar = [
            rct.RaceTrigger(
                session_key=1, round=1, name="Test GP",
                lock_at=far_future_lock,
                trigger_at=far_future_lock - timedelta(seconds=300),
            )
        ]
        fired = []
        trigger = rct.RaceCalendarTrigger(on_trigger=fired.append, calendar=calendar)
        count = trigger.run(max_iterations=2, poll_interval_seconds=0)

        assert count == 0
        assert fired == []

    def test_run_wall_clock_fires_due_trigger(self):
        now = datetime.now(timezone.utc)
        due_lock = now + timedelta(seconds=1)
        calendar = [
            rct.RaceTrigger(
                session_key=1, round=1, name="Test GP",
                lock_at=due_lock,
                trigger_at=now - timedelta(seconds=1),  # already due
            )
        ]
        fired = []
        trigger = rct.RaceCalendarTrigger(on_trigger=fired.append, calendar=calendar)
        count = trigger.run(max_iterations=1, poll_interval_seconds=0)

        assert count == 1
        assert fired[0].session_key == 1

    def test_calendar_param_bypasses_openf1_call(self):
        """A caller-supplied calendar (e.g. persona runner reusing an
        already-fetched calendar) must not hit OpenF1 again."""
        calendar = [
            rct.RaceTrigger(
                session_key=1, round=1, name="Test GP",
                lock_at=datetime.now(timezone.utc),
                trigger_at=datetime.now(timezone.utc),
            )
        ]
        trigger = rct.RaceCalendarTrigger(on_trigger=lambda t: None, calendar=calendar)
        assert trigger.calendar == calendar
