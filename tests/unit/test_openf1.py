"""Unit tests for the OpenF1 client (F1-01, F1-02, F1-07).

These never touch the network — the old Jolpica tests did, which is why six of
them fail with HTTP 429 whenever the upstream rate-limits us. Everything here
is driven by `responses` fixtures.
"""

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import requests
import responses

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import openf1  # noqa: E402

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
    """Retries are real in production but shouldn't slow the suite down."""
    monkeypatch.setattr(openf1.time, "sleep", lambda _s: None)


@pytest.fixture(autouse=True)
def network_mocked(monkeypatch):
    """conftest sets OPENF1_OFFLINE for the app tests; these tests exercise the
    request path itself, with `responses` standing in for the network."""
    monkeypatch.setattr(openf1, "OFFLINE", False)


SESSIONS = [
    {"session_key": 200, "session_type": "Race", "date_start": "2026-03-08T04:00:00+00:00",
     "circuit_short_name": "Melbourne", "country_name": "Australia", "is_cancelled": False},
    {"session_key": 100, "session_type": "Race", "date_start": "2026-03-01T04:00:00+00:00",
     "circuit_short_name": "Sakhir", "country_name": "Bahrain", "is_cancelled": False},
    {"session_key": 300, "session_type": "Race", "date_start": "2026-03-15T04:00:00+00:00",
     "circuit_short_name": "Suzuka", "country_name": "Japan", "is_cancelled": True},
]

RESULTS = [
    {"position": 2, "driver_number": 12, "dnf": False},
    {"position": 1, "driver_number": 63, "dnf": False},
    {"position": 3, "driver_number": 16, "dnf": False},
]

DRIVERS = [
    {"driver_number": 63, "full_name": "George RUSSELL", "name_acronym": "RUS", "team_name": "Mercedes"},
    {"driver_number": 12, "full_name": "Kimi ANTONELLI", "name_acronym": "ANT", "team_name": "Mercedes"},
    {"driver_number": 16, "full_name": "Charles LECLERC", "name_acronym": "LEC", "team_name": "Ferrari"},
]


class TestSessions:
    @responses.activate
    def test_races_sorted_and_round_numbered(self, db):
        responses.add(responses.GET, f"{BASE}/sessions", json=SESSIONS, status=200)
        data = openf1.get_race_sessions(season=2026, db=db).data
        assert [s["session_key"] for s in data] == [100, 200]
        assert [s["round"] for s in data] == [1, 2]

    @responses.activate
    def test_cancelled_races_excluded(self, db):
        responses.add(responses.GET, f"{BASE}/sessions", json=SESSIONS, status=200)
        keys = [s["session_key"] for s in openf1.get_race_sessions(season=2026, db=db).data]
        assert 300 not in keys


class TestPodium:
    @responses.activate
    def test_podium_resolves_driver_names(self, db):
        responses.add(responses.GET, f"{BASE}/session_result", json=RESULTS, status=200)
        responses.add(responses.GET, f"{BASE}/drivers", json=DRIVERS, status=200)
        podium = openf1.get_podium(200, db=db)
        assert podium["p1"]["driver_name"] == "George Russell"
        assert podium["p2"]["driver_name"] == "Kimi Antonelli"
        assert podium["p3"]["constructor"] == "Ferrari"

    @responses.activate
    def test_incomplete_race_returns_none_not_error(self, db):
        """A race in progress is 'no podium yet', not a failure."""
        responses.add(responses.GET, f"{BASE}/session_result",
                      json=[{"position": 1, "driver_number": 63}], status=200)
        assert openf1.get_podium(200, db=db) is None

    @responses.activate
    def test_unknown_driver_number_falls_back_to_number(self, db):
        responses.add(responses.GET, f"{BASE}/session_result", json=RESULTS, status=200)
        responses.add(responses.GET, f"{BASE}/drivers", json=[], status=200)
        podium = openf1.get_podium(200, db=db)
        assert podium["p1"]["driver_name"] == "#63"


class TestCacheFallback:
    """F1-02: upstream failure must serve last-known-good, not an error."""

    @responses.activate
    def test_successful_read_is_cached(self, db):
        responses.add(responses.GET, f"{BASE}/session_result", json=RESULTS, status=200)
        openf1.get_session_result(200, db=db)
        row = db.execute("SELECT COUNT(*) c FROM api_cache").fetchone()
        assert row["c"] == 1

    @responses.activate
    def test_upstream_failure_serves_cache(self, db):
        responses.add(responses.GET, f"{BASE}/session_result", json=RESULTS, status=200)
        openf1.get_session_result(200, db=db)
        responses.reset()
        responses.add(responses.GET, f"{BASE}/session_result", status=500)
        cached = openf1.get_session_result(200, db=db)
        assert cached.from_cache is True
        assert [r["position"] for r in cached.data] == [1, 2, 3]

    @responses.activate
    def test_rate_limit_serves_cache(self, db):
        """The exact failure that breaks the current Jolpica-based tests."""
        responses.add(responses.GET, f"{BASE}/sessions", json=SESSIONS, status=200)
        openf1.get_race_sessions(season=2026, db=db)
        responses.reset()
        responses.add(responses.GET, f"{BASE}/sessions", status=429)
        assert openf1.get_race_sessions(season=2026, db=db).from_cache is True

    @responses.activate
    def test_failure_without_cache_raises(self, db):
        responses.add(responses.GET, f"{BASE}/session_result", status=500)
        with pytest.raises(openf1.OpenF1Error):
            openf1.get_session_result(999, db=db)

    @responses.activate
    def test_rate_limit_exhaustion_falls_through_to_cache_not_error(self, db):
        """AC: being rate-limited must never surface an error when a
        last-known-good cache entry exists — exactly like an outage."""
        responses.add(responses.GET, f"{BASE}/session_result", json=RESULTS, status=200)
        openf1.get_session_result(200, db=db)
        responses.reset()
        for _ in range(openf1.RETRY_ATTEMPTS):
            responses.add(responses.GET, f"{BASE}/session_result", status=429)
        result = openf1.get_session_result(200, db=db)
        assert result.from_cache is True
        assert [r["position"] for r in result.data] == [1, 2, 3]

    @responses.activate
    def test_error_carries_status_code_for_outcome_classification(self, db):
        """BUD-125: OpenF1Error exposes status_code so callers can record a
        specific fetch_attempts outcome (e.g. rate_limited) without parsing
        the message string."""
        responses.add(responses.GET, f"{BASE}/session_result", status=429)
        with pytest.raises(openf1.OpenF1Error) as exc_info:
            openf1.get_session_result(999, db=db)
        assert exc_info.value.status_code == 429
        assert exc_info.value.is_timeout is False

    def test_error_carries_is_timeout(self, db, monkeypatch):
        def raise_timeout(*args, **kwargs):
            raise requests.Timeout("timed out")

        monkeypatch.setattr(openf1.requests, "get", raise_timeout)
        with pytest.raises(openf1.OpenF1Error) as exc_info:
            openf1.get_session_result(999, db=db)
        assert exc_info.value.is_timeout is True
        assert exc_info.value.status_code is None

    @responses.activate
    def test_retries_before_giving_up(self, db):
        responses.add(responses.GET, f"{BASE}/session_result", status=500)
        responses.add(responses.GET, f"{BASE}/session_result", status=500)
        responses.add(responses.GET, f"{BASE}/session_result", json=RESULTS, status=200)
        result = openf1.get_session_result(200, db=db)
        assert result.from_cache is False
        assert len(responses.calls) == 3

    @responses.activate
    def test_cache_keys_are_per_request(self, db):
        responses.add(responses.GET, f"{BASE}/session_result", json=RESULTS, status=200)
        openf1.get_session_result(200, db=db)
        openf1.get_session_result(201, db=db)
        assert db.execute("SELECT COUNT(*) c FROM api_cache").fetchone()["c"] == 2

    def test_corrupt_cache_entry_is_ignored(self, db):
        db.execute("INSERT INTO api_cache VALUES (?,?,?)",
                   ("sessions?x=1", "{not json", datetime.now(timezone.utc).isoformat()))
        db.commit()
        assert openf1._cache_read(db, "sessions?x=1") is None


class TestRateLimitBackoff:
    """F1-08 / BUD-167: a 429 gets its own Retry-After-aware, budgeted
    backoff instead of the linear 1.5s*attempt used for a general transport
    failure."""

    @responses.activate
    def test_429_with_header_honors_retry_after(self, db, monkeypatch):
        monkeypatch.setattr(openf1, "MIN_REQUEST_INTERVAL_SEC", 0)  # isolate from the rate ceiling
        sleeps = []
        monkeypatch.setattr(openf1.time, "sleep", lambda s: sleeps.append(s))
        responses.add(responses.GET, f"{BASE}/session_result",
                      status=429, headers={"Retry-After": "30"})
        responses.add(responses.GET, f"{BASE}/session_result", json=RESULTS, status=200)

        result = openf1.get_session_result(200, db=db)

        assert result.from_cache is False
        assert sleeps == [30.0]  # honored in full, not the linear 1.5s

    @responses.activate
    def test_429_without_header_uses_exponential_jitter_not_linear(self, db, monkeypatch):
        monkeypatch.setattr(openf1, "MIN_REQUEST_INTERVAL_SEC", 0)  # isolate from the rate ceiling
        sleeps = []
        monkeypatch.setattr(openf1.time, "sleep", lambda s: sleeps.append(s))
        # Full jitter: pin random.uniform(0, x) -> x for a deterministic assertion.
        monkeypatch.setattr(openf1.random, "uniform", lambda lo, hi: hi)
        responses.add(responses.GET, f"{BASE}/session_result", status=429)
        responses.add(responses.GET, f"{BASE}/session_result", status=429)
        responses.add(responses.GET, f"{BASE}/session_result", json=RESULTS, status=200)

        result = openf1.get_session_result(200, db=db)

        assert result.from_cache is False
        base = openf1.RATE_LIMIT_BACKOFF_BASE_SEC
        assert sleeps == [base, base * 2]  # doubles per attempt
        assert sleeps != [openf1.RETRY_BACKOFF_SEC * 1, openf1.RETRY_BACKOFF_SEC * 2]

    @responses.activate
    def test_429_with_cache_serves_cache_not_error(self, db, monkeypatch):
        """429-with-cache: exhausting the rate-limit budget must fall
        through to last-known-good exactly like an outage does."""
        monkeypatch.setattr(openf1.time, "sleep", lambda s: None)
        responses.add(responses.GET, f"{BASE}/sessions", json=SESSIONS, status=200)
        openf1.get_race_sessions(season=2026, db=db)
        responses.reset()
        for _ in range(openf1.RETRY_ATTEMPTS):
            responses.add(responses.GET, f"{BASE}/sessions", status=429)

        result = openf1.get_race_sessions(season=2026, db=db)

        assert result.from_cache is True

    @responses.activate
    def test_retry_budget_bounded_below_cron_window(self, db, monkeypatch):
        """A single oversized Retry-After must not be honored past the
        configured budget — the call fails fast to cache/error instead of
        sleeping long enough to overrun the CronJob's run window."""
        monkeypatch.setattr(openf1, "MIN_REQUEST_INTERVAL_SEC", 0)  # isolate from the rate ceiling
        sleeps = []
        monkeypatch.setattr(openf1.time, "sleep", lambda s: sleeps.append(s))
        monkeypatch.setattr(openf1, "RATE_LIMIT_MAX_WAIT_SEC", 60.0)
        responses.add(responses.GET, f"{BASE}/session_result",
                      status=429, headers={"Retry-After": "500"})
        for _ in range(openf1.RETRY_ATTEMPTS - 1):
            responses.add(responses.GET, f"{BASE}/session_result",
                          status=429, headers={"Retry-After": "500"})

        with pytest.raises(openf1.OpenF1Error) as exc_info:
            openf1.get_session_result(999, db=db)

        assert sleeps == []  # gave up immediately rather than sleeping 500s
        assert exc_info.value.status_code == 429
        assert "rate limit" in str(exc_info.value).lower()

    @responses.activate
    def test_429_error_message_and_status_distinguish_from_transport_failure(self, db, monkeypatch):
        """AC: different log lines / exception detail for 429 vs a general
        transport failure, and OpenF1Error still carries the 429 marker
        BUD-125's outcome_for_error() keys off."""
        monkeypatch.setattr(openf1.time, "sleep", lambda s: None)
        for _ in range(openf1.RETRY_ATTEMPTS):
            responses.add(responses.GET, f"{BASE}/session_result", status=429)

        with pytest.raises(openf1.OpenF1Error) as rate_limited_exc:
            openf1.get_session_result(998, db=db)

        responses.reset()
        for _ in range(openf1.RETRY_ATTEMPTS):
            responses.add(responses.GET, f"{BASE}/session_result", status=500)

        with pytest.raises(openf1.OpenF1Error) as transport_exc:
            openf1.get_session_result(999, db=db)

        assert "rate limit" in str(rate_limited_exc.value).lower()
        assert "rate limit" not in str(transport_exc.value).lower()
        assert rate_limited_exc.value.status_code == 429
        assert transport_exc.value.status_code == 500

    @responses.activate
    def test_403_with_quota_body_treated_as_rate_limited(self, db, monkeypatch):
        """OpenF1 has been observed returning 403 for quota exhaustion; when
        the body says so, treat it the same as a 429."""
        monkeypatch.setattr(openf1.time, "sleep", lambda s: None)
        for _ in range(openf1.RETRY_ATTEMPTS):
            responses.add(responses.GET, f"{BASE}/session_result",
                          status=403, json={"error": "quota exceeded"})

        with pytest.raises(openf1.OpenF1Error) as exc_info:
            openf1.get_session_result(997, db=db)

        assert "rate limit" in str(exc_info.value).lower()

    @responses.activate
    def test_plain_403_is_not_treated_as_rate_limited(self, db, monkeypatch):
        """A 403 with no quota signal must stay on the general failure path
        (linear backoff, generic error message)."""
        monkeypatch.setattr(openf1.time, "sleep", lambda s: None)
        for _ in range(openf1.RETRY_ATTEMPTS):
            responses.add(responses.GET, f"{BASE}/session_result",
                          status=403, json={"error": "forbidden"})

        with pytest.raises(openf1.OpenF1Error) as exc_info:
            openf1.get_session_result(996, db=db)

        assert "rate limit" not in str(exc_info.value).lower()


class TestRequestRateCeiling:
    """F1-08 / BUD-167: a process-wide floor on request spacing so a dev
    loop or test run can't burst past OpenF1's documented limit."""

    def test_default_matches_documented_openf1_limit(self):
        # OpenF1 maintainer: "30 [requests] every 10 seconds"
        # (github.com/br-g/openf1 issue #113, 2024-10-26).
        assert openf1.MIN_REQUEST_INTERVAL_SEC == pytest.approx(10 / 30)

    @responses.activate
    def test_back_to_back_requests_are_spaced_by_the_floor(self, db, monkeypatch):
        sleeps = []
        monkeypatch.setattr(openf1.time, "sleep", lambda s: sleeps.append(s))
        monkeypatch.setattr(openf1, "MIN_REQUEST_INTERVAL_SEC", 5.0)
        monkeypatch.setattr(openf1, "_last_request_monotonic", None)
        responses.add(responses.GET, f"{BASE}/sessions", json=SESSIONS, status=200)
        responses.add(responses.GET, f"{BASE}/meetings", json=[], status=200)

        openf1.get_race_sessions(season=2026, db=db)
        openf1.get_meetings(season=2026, db=db)

        assert any(s > 0 for s in sleeps)

    @responses.activate
    def test_zero_interval_disables_the_ceiling(self, db, monkeypatch):
        sleeps = []
        monkeypatch.setattr(openf1.time, "sleep", lambda s: sleeps.append(s))
        monkeypatch.setattr(openf1, "MIN_REQUEST_INTERVAL_SEC", 0)
        monkeypatch.setattr(openf1, "_last_request_monotonic", None)
        responses.add(responses.GET, f"{BASE}/sessions", json=SESSIONS, status=200)
        responses.add(responses.GET, f"{BASE}/meetings", json=[], status=200)

        openf1.get_race_sessions(season=2026, db=db)
        openf1.get_meetings(season=2026, db=db)

        assert sleeps == []

    def test_env_tunable(self, monkeypatch):
        """OPENF1_MIN_REQUEST_INTERVAL_SEC actually drives the module
        constant, not just an unused env var."""
        monkeypatch.setenv("OPENF1_MIN_REQUEST_INTERVAL_SEC", "0.75")
        import importlib
        try:
            reloaded = importlib.reload(openf1)
            assert reloaded.MIN_REQUEST_INTERVAL_SEC == pytest.approx(0.75)
        finally:
            importlib.reload(openf1)  # restore real env-derived defaults


class TestDataAge:
    def test_fresh_result_not_stale(self):
        r = openf1.CachedResult(data=[], fetched_at=datetime.now(timezone.utc))
        assert r.is_stale is False
        assert r.age_label() == "just now"

    def test_old_cache_is_stale(self):
        old = datetime.now(timezone.utc) - timedelta(seconds=openf1.CACHE_STALE_AFTER_SEC + 60)
        r = openf1.CachedResult(data=[], fetched_at=old, from_cache=True)
        assert r.is_stale is True

    def test_live_data_is_never_stale(self):
        """Only cached payloads carry a staleness warning."""
        old = datetime.now(timezone.utc) - timedelta(days=5)
        assert openf1.CachedResult(data=[], fetched_at=old, from_cache=False).is_stale is False

    @pytest.mark.parametrize("secs,expected", [
        (30, "just now"), (600, "10 min ago"), (7200, "2h ago"), (172800, "2d ago"),
    ])
    def test_age_labels(self, secs, expected):
        stamp = datetime.now(timezone.utc) - timedelta(seconds=secs)
        assert openf1.CachedResult(data=[], fetched_at=stamp).age_label() == expected


class TestSafetyCar:
    """F1-07: safety-car facts extracted from race_control."""

    @responses.activate
    def test_counts_deployments_not_endings(self, db):
        responses.add(responses.GET, f"{BASE}/race_control", json=[
            {"category": "SafetyCar", "message": "SAFETY CAR DEPLOYED"},
            {"category": "SafetyCar", "message": "SAFETY CAR IN THIS LAP"},
            {"category": "Flag", "message": "YELLOW"},
        ], status=200)
        s = openf1.get_safety_car_summary(200, db=db)
        assert s["safety_car_count"] == 1
        assert s["had_safety_car"] is True

    @responses.activate
    def test_vsc_counted_separately_from_full_sc(self, db):
        responses.add(responses.GET, f"{BASE}/race_control", json=[
            {"category": "SafetyCar", "message": "VSC DEPLOYED"},
            {"category": "SafetyCar", "message": "VSC ENDING"},
            {"category": "SafetyCar", "message": "VSC DEPLOYED"},
        ], status=200)
        s = openf1.get_safety_car_summary(200, db=db)
        assert s["virtual_safety_car_count"] == 2
        assert s["had_safety_car"] is False       # no *full* safety car
        assert s["had_any_safety_car"] is True    # but a user's yes/no was "yes"

    @responses.activate
    def test_clean_race(self, db):
        responses.add(responses.GET, f"{BASE}/race_control",
                      json=[{"category": "Flag", "message": "GREEN"}], status=200)
        s = openf1.get_safety_car_summary(200, db=db)
        assert s["had_any_safety_car"] is False
        assert s["safety_car_count"] == 0


class TestDriverNames:
    @pytest.mark.parametrize("driver,expected", [
        ({"full_name": "George RUSSELL"}, "George Russell"),
        ({"full_name": "Andrea Kimi ANTONELLI"}, "Andrea Kimi Antonelli"),
        ({"first_name": "Max", "last_name": "Verstappen"}, "Max Verstappen"),
        ({"name_acronym": "HAM"}, "HAM"),
        ({}, "Unknown"),
    ])
    def test_display_names(self, driver, expected):
        assert openf1.driver_display_name(driver) == expected
