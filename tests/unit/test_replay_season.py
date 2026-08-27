"""Unit tests for F1-110 season replay harness and F1-111 is_synthetic column."""

from __future__ import annotations

import os
import sys

# Set test environment BEFORE any imports
os.environ["DATABASE_PATH"] = ":memory:"
os.environ["F1_SEASON"] = "2026"
os.environ["OPENF1_OFFLINE"] = "true"
os.environ["TESTING"] = "true"

_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, _root)
sys.path.insert(0, os.path.join(_root, "src"))
sys.path.insert(0, os.path.join(_root, "scripts"))

import pytest

import app as app_module
from scripts.replay_season import (
    ReplayError,
    _delete_synthetic_users,
    _synthetic_uuid,
    run_replay,
    run_teardown,
)


@pytest.fixture
def db():
    """Provide a freshly initialized in-memory DB for replay tests."""
    with app_module.app.app_context():
        app_module.init_db()
        db = app_module.get_db()
        yield db


def _insert_drivers(db, count: int = 20) -> list[int]:
    ids = []
    for i in range(1, count + 1):
        db.execute(
            "INSERT INTO drivers (id, driver_id, name, number) VALUES (?, ?, ?, ?)",
            (i, f"drv{i}", f"Driver {i}", i),
        )
        ids.append(i)
    db.commit()
    return ids


def _insert_races_and_results(db, season: int = 2026, count: int = 12) -> list[int]:
    race_ids = []
    for i in range(1, count + 1):
        db.execute(
            "INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)",
            (f"Race {i}", i, f"{season}-05-{i:02d} 14:00:00", "completed"),
        )
        race_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        # Deterministic podium rotating through first 6 drivers
        p1 = ((i - 1) % 6) + 1
        p2 = ((i) % 6) + 1
        p3 = ((i + 1) % 6) + 1
        db.execute(
            "INSERT INTO results (race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?)",
            (race_id, p1, p2, p3),
        )
        race_ids.append(race_id)
    db.commit()
    return race_ids


class TestSyntheticColumn:
    """F1-111: users.is_synthetic ships with the first persona."""

    def test_users_table_has_is_synthetic_column(self, db):
        columns = {
            row["name"]
            for row in db.execute("PRAGMA table_info(users)").fetchall()
        }
        assert "is_synthetic" in columns

    def test_normal_users_default_to_not_synthetic(self, db):
        db.execute(
            "INSERT INTO users (session_id, username) VALUES (?, ?)",
            ("real-user-1", "brett"),
        )
        db.commit()
        row = db.execute("SELECT is_synthetic FROM users WHERE session_id = ?", ("real-user-1",)).fetchone()
        assert row["is_synthetic"] == 0

    def test_synthetic_users_flagged(self, db):
        db.execute(
            "INSERT INTO users (session_id, username, is_synthetic) VALUES (?, ?, ?)",
            ("syn-1", "synthetic_001", 1),
        )
        db.commit()
        row = db.execute("SELECT is_synthetic FROM users WHERE session_id = ?", ("syn-1",)).fetchone()
        assert row["is_synthetic"] == 1


class TestReplayHarness:
    """F1-110: replay harness on the real scoring path."""

    def test_replay_requires_at_least_ten_scored_races(self, db):
        _insert_drivers(db)
        _insert_races_and_results(db, count=5)
        with pytest.raises(ReplayError):
            run_replay(2026, 1, db=db)

    def test_replay_creates_synthetic_users_predictions_and_scores(self, db):
        _insert_drivers(db)
        _insert_races_and_results(db, count=12)

        counts_before = {
            "users": db.execute("SELECT COUNT(*) FROM users").fetchone()[0],
            "predictions": db.execute("SELECT COUNT(*) FROM predictions").fetchone()[0],
            "scores": db.execute("SELECT COUNT(*) FROM scores").fetchone()[0],
        }

        result = run_replay(2026, seed=42, db=db)

        assert len(result["users"]) == 25
        assert len(result["races"]) == 12
        leaderboard = result["leaderboard"]
        assert len(leaderboard) == 25

        synthetic_count = db.execute(
            "SELECT COUNT(*) FROM users WHERE is_synthetic = 1"
        ).fetchone()[0]
        assert synthetic_count == 25

        predictions_count = db.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
        scores_count = db.execute("SELECT COUNT(*) FROM scores").fetchone()[0]
        assert predictions_count == counts_before["predictions"] + 25 * 12
        assert scores_count == counts_before["scores"] + 25 * 12

    def test_replay_is_deterministic_for_same_seed(self, db):
        _insert_drivers(db)
        _insert_races_and_results(db, count=10)

        result_a = run_replay(2026, seed=7, db=db)
        users_a = [(u[0], u[1]) for u in result_a["users"]]
        board_a = [(row["username"], row["total_score"]) for row in result_a["leaderboard"]]

        # Teardown and rerun with same seed
        run_teardown(db=db)
        result_b = run_replay(2026, seed=7, db=db)
        users_b = [(u[0], u[1]) for u in result_b["users"]]
        board_b = [(row["username"], row["total_score"]) for row in result_b["leaderboard"]]

        assert users_a == users_b
        assert board_a == board_b

    def test_different_seed_produces_different_distribution(self, db):
        _insert_drivers(db)
        _insert_races_and_results(db, count=10)

        result_1 = run_replay(2026, seed=1, db=db)
        board_1 = [(row["username"], row["total_score"]) for row in result_1["leaderboard"]]

        run_teardown(db=db)
        result_2 = run_replay(2026, seed=2, db=db)
        board_2 = [(row["username"], row["total_score"]) for row in result_2["leaderboard"]]

        assert board_1 != board_2

    def test_teardown_restores_pre_replay_counts(self, db):
        _insert_drivers(db)
        _insert_races_and_results(db, count=11)

        counts_before = {
            "users": db.execute("SELECT COUNT(*) FROM users").fetchone()[0],
            "predictions": db.execute("SELECT COUNT(*) FROM predictions").fetchone()[0],
            "scores": db.execute("SELECT COUNT(*) FROM scores").fetchone()[0],
        }

        run_replay(2026, seed=99, db=db)
        teardown = run_teardown(db=db)

        assert teardown["deleted_users"] == 25
        assert teardown["deleted_predictions"] == 25 * 11
        assert teardown["deleted_scores"] == 25 * 11
        assert teardown["counts_after"] == counts_before

    def test_replay_uses_production_calculate_score(self, db):
        """Corrupt one stored result; replay scores must shift exactly like live scoring."""
        _insert_drivers(db)
        race_ids = _insert_races_and_results(db, count=10)

        # Capture scores with the original result
        result_1 = run_replay(2026, seed=123, db=db)
        original_scores = {
            row["username"]: row["total_score"] for row in result_1["leaderboard"]
        }

        # Corrupt the first race result: swap P1 and P2
        first_race = race_ids[0]
        row = db.execute(
            "SELECT p1_driver_id, p2_driver_id, p3_driver_id FROM results WHERE race_id = ?",
            (first_race,),
        ).fetchone()
        db.execute(
            "UPDATE results SET p1_driver_id = ?, p2_driver_id = ? WHERE race_id = ?",
            (row["p2_driver_id"], row["p1_driver_id"], first_race),
        )
        db.commit()

        # Replay again with the same seed; scores must differ for at least one user
        run_teardown(db=db)
        result_2 = run_replay(2026, seed=123, db=db)
        new_scores = {
            row["username"]: row["total_score"] for row in result_2["leaderboard"]
        }

        assert new_scores != original_scores

    def test_synthetic_uuids_are_deterministic(self):
        assert _synthetic_uuid(2026, 42, 0) == _synthetic_uuid(2026, 42, 0)
        assert _synthetic_uuid(2026, 42, 0) != _synthetic_uuid(2026, 42, 1)
        assert _synthetic_uuid(2026, 42, 0) != _synthetic_uuid(2026, 43, 0)

    def test_delete_synthetic_users_is_idempotent(self, db):
        _insert_drivers(db)
        _insert_races_and_results(db, count=10)
        run_replay(2026, seed=5, db=db)

        first = _delete_synthetic_users(db)
        second = _delete_synthetic_users(db)

        assert first[0] == 25  # users
        assert second == (0, 0, 0)

    def test_different_seed_does_not_conflict_with_existing_synthetic_users(self, db):
        """BUD-132 replay fix: usernames must be seed-scoped so a second seed
        run inserts new users instead of being silently skipped by UNIQUE."""
        _insert_drivers(db)
        _insert_races_and_results(db, count=10)

        result_1 = run_replay(2026, seed=1, db=db)
        result_2 = run_replay(2026, seed=2, db=db)

        # Both replays created their own 25 users
        assert len(result_1["users"]) == 25
        assert len(result_2["users"]) == 25

        # No cross-seed username overlap
        usernames_1 = {u[1] for u in result_1["users"]}
        usernames_2 = {u[1] for u in result_2["users"]}
        assert not usernames_1.intersection(usernames_2)

        # All 50 synthetic users are in the DB (not silently skipped)
        synthetic_count = db.execute(
            "SELECT COUNT(*) FROM users WHERE is_synthetic = 1"
        ).fetchone()[0]
        assert synthetic_count == 50

    def test_replay_import_does_not_load_app_module(self):
        """BUD-132 replay fix: importing the replay module must not eagerly
        import src.app (and therefore must not trigger init_db()) before the
        CLI has had a chance to parse --db."""
        import importlib
        import sys

        # Ensure a clean import observation.
        module_name = "scripts.replay_season"
        sys.modules.pop(module_name, None)
        sys.modules.pop("app", None)

        importlib.import_module(module_name)

        assert "app" not in sys.modules
        assert sys.modules[module_name]._app_module is None
