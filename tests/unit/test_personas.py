"""Unit tests for F1-112 persona archetypes."""

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

from personas import (
    Contrarian,
    FrontRunner,
    HomeTrackHero,
    MaxVerstappenFanboy,
    MidfieldOracle,
    RandomPicker,
    RaceContext,
    StatisticsJunkie,
    assign_personas,
    generate_persona_prediction,
    get_persona,
    list_persona_slugs,
)
from scripts.replay_season import ReplayError, run_replay, run_teardown

import app as app_module


@pytest.fixture
def db():
    """Provide a freshly initialized in-memory DB for persona tests."""
    with app_module.app.app_context():
        app_module.init_db()
        db = app_module.get_db()
        yield db


def _insert_drivers(db, count: int = 20, include_max: bool = False) -> list[int]:
    ids = []
    for i in range(1, count + 1):
        db.execute(
            "INSERT INTO drivers (id, driver_id, name, number) VALUES (?, ?, ?, ?)",
            (i, f"drv{i}", f"Driver {i}", i),
        )
        ids.append(i)
    if include_max:
        max_id = count + 1
        db.execute(
            "INSERT INTO drivers (id, driver_id, name, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?)",
            (max_id, "max_verstappen", "Max Verstappen", 33, "VER", "Dutch"),
        )
        ids.append(max_id)
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


def _make_context(db, race_name: str = "Race 1", round_: int = 1) -> RaceContext:
    drivers = [
        {
            "id": row["id"],
            "driver_id": row["driver_id"],
            "name": row["name"],
            "team": row["team"],
            "number": row["number"],
            "code": row["code"],
            "nationality": row["nationality"],
        }
        for row in db.execute(
            "SELECT id, driver_id, name, team, number, code, nationality FROM drivers ORDER BY id"
        ).fetchall()
    ]
    return RaceContext(
        race_id=1,
        race_name=race_name,
        round=round_,
        date="2026-05-01 14:00:00",
        drivers=tuple(drivers),
    )


class TestPersonaRegistry:
    """F1-112 AC: seven persona archetypes are defined and discoverable."""

    def test_seven_canonical_personas_exist(self):
        slugs = list_persona_slugs()
        assert len(slugs) == 7
        assert slugs == [
            "random_picker",
            "front_runner",
            "midfield_oracle",
            "contrarian",
            "max_verstappen_fanboy",
            "statistics_junkie",
            "home_track_hero",
        ]

    def test_get_persona_by_slug(self):
        assert get_persona("front_runner").slug == "front_runner"
        with pytest.raises(KeyError):
            get_persona("chaos_picker")  # alias not registered

    def test_assign_personas_mixed_cycles_all_seven(self):
        slugs = assign_personas(14, mixed=True)
        assert len(slugs) == 14
        assert slugs[:7] == list_persona_slugs()
        assert slugs[7:] == list_persona_slugs()

    def test_assign_personas_single_slug(self):
        assert assign_personas(3, persona_slug="contrarian") == ["contrarian"] * 3

    def test_assign_personas_default_is_random_picker(self):
        assert assign_personas(5) == ["random_picker"] * 5


class TestPersonaStrategies:
    """Each persona maps a race entry list to deterministic P1/P2/P3 picks."""

    def test_random_picker_chooses_three_distinct_drivers(self, db):
        _insert_drivers(db, count=10)
        context = _make_context(db)
        persona = RandomPicker()
        picks = {persona.pick(context, __import__("random").Random(i)) for i in range(20)}
        # At least some variety across seeds.
        assert len(picks) > 1
        for pick in picks:
            assert len(set(pick)) == 3

    def test_front_runner_picks_lowest_numbers(self, db):
        _insert_drivers(db, count=10)
        context = _make_context(db)
        persona = FrontRunner()
        assert persona.pick(context, None) == (1, 2, 3)

    def test_midfield_oracle_picks_middle_of_grid(self, db):
        _insert_drivers(db, count=10)
        context = _make_context(db)
        persona = MidfieldOracle()
        # For 10 drivers sorted by number, middle starts at index 5 -> drivers 6,7,8
        assert persona.pick(context, None) == (6, 7, 8)

    def test_contrarian_picks_highest_numbers(self, db):
        _insert_drivers(db, count=10)
        context = _make_context(db)
        persona = Contrarian()
        assert persona.pick(context, None) == (10, 9, 8)

    def test_max_verstappen_fanboy_picks_max_p1(self, db):
        _insert_drivers(db, count=10, include_max=True)
        context = _make_context(db)
        persona = MaxVerstappenFanboy()
        p1, p2, p3 = persona.pick(context, __import__("random").Random(0))
        assert p1 == 11
        assert len({p1, p2, p3}) == 3
        assert p1 not in {p2, p3}

    def test_max_verstappen_fanboy_falls_back_when_max_absent(self, db):
        _insert_drivers(db, count=10)
        context = _make_context(db)
        persona = MaxVerstappenFanboy()
        pick = persona.pick(context, __import__("random").Random(0))
        assert len(set(pick)) == 3

    def test_statistics_junkie_picks_closest_to_round(self, db):
        _insert_drivers(db, count=20)
        context = _make_context(db, round_=5)
        persona = StatisticsJunkie()
        # Driver 5 has diff 0, drivers 4 and 6 have diff 1.
        assert persona.pick(context, None) == (5, 4, 6)

    def test_home_track_hero_picks_local_drivers(self, db):
        # Insert a Dutch driver and use Dutch GP
        db.execute(
            "INSERT INTO drivers (id, driver_id, name, number, nationality) VALUES (?, ?, ?, ?, ?)",
            (1, "dutch_driver", "Dutch Driver", 5, "Dutch"),
        )
        for i in range(2, 6):
            db.execute(
                "INSERT INTO drivers (id, driver_id, name, number, nationality) VALUES (?, ?, ?, ?, ?)",
                (i, f"drv{i}", f"Driver {i}", i * 10, "British"),
            )
        db.commit()
        context = _make_context(db, race_name="Dutch GP")
        persona = HomeTrackHero()
        p1, p2, p3 = persona.pick(context, __import__("random").Random(0))
        assert p1 == 1
        assert len({p1, p2, p3}) == 3


class TestPersonaDeterminism:
    """Persona picks are deterministic for the same inputs."""

    def test_generate_persona_prediction_is_deterministic(self, db):
        _insert_drivers(db, count=10)
        context = _make_context(db)
        a = generate_persona_prediction(42, "user-1", "random_picker", context)
        b = generate_persona_prediction(42, "user-1", "random_picker", context)
        assert a == b

    def test_different_users_get_different_picks(self, db):
        _insert_drivers(db, count=10)
        context = _make_context(db)
        a = generate_persona_prediction(42, "user-a", "random_picker", context)
        b = generate_persona_prediction(42, "user-b", "random_picker", context)
        assert a != b


class TestReplayPersonaIntegration:
    """Replay harness supports --persona and --mixed-personas."""

    def test_run_replay_with_single_persona(self, db):
        _insert_drivers(db, count=20)
        _insert_races_and_results(db, count=12)
        result = run_replay(2026, seed=42, db=db, user_count=5, persona_slug="front_runner")
        assert len(result["users"]) == 5
        assert all(u[2] == "front_runner" for u in result["users"])

        rows = db.execute("SELECT persona FROM users WHERE is_synthetic = 1").fetchall()
        assert {row["persona"] for row in rows} == {"front_runner"}

    def test_run_replay_with_mixed_personas(self, db):
        _insert_drivers(db, count=20)
        _insert_races_and_results(db, count=12)
        result = run_replay(2026, seed=42, db=db, user_count=14, mixed_personas=True)
        slugs = [u[2] for u in result["users"]]
        assert slugs == list_persona_slugs() + list_persona_slugs()

    def test_run_replay_rejects_unknown_persona(self, db):
        _insert_drivers(db, count=20)
        _insert_races_and_results(db, count=12)
        with pytest.raises(ReplayError):
            run_replay(2026, seed=42, db=db, persona_slug="not_a_persona")

    def test_default_replay_preserves_legacy_seed_behavior(self, db):
        """BUD-134: default replay (no persona args) is unchanged from BUD-132."""
        _insert_drivers(db, count=20)
        _insert_races_and_results(db, count=12)

        def _current_picks():
            return {
                (row["user_id"], row["race_id"]): (
                    row["p1_driver_id"],
                    row["p2_driver_id"],
                    row["p3_driver_id"],
                )
                for row in db.execute(
                    "SELECT user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id FROM predictions"
                ).fetchall()
            }

        run_replay(2026, seed=42, db=db, user_count=5)
        first_picks = _current_picks()

        run_teardown(db=db)
        run_replay(2026, seed=42, db=db, user_count=5)
        second_picks = _current_picks()

        assert first_picks == second_picks

    def test_default_replay_does_not_use_persona_path(self, db):
        """Default replay stores the random_picker persona but uses legacy RNG."""
        _insert_drivers(db, count=20)
        _insert_races_and_results(db, count=12)

        run_replay(2026, seed=42, db=db, user_count=5)
        personas = {row["persona"] for row in db.execute("SELECT persona FROM users WHERE is_synthetic = 1").fetchall()}
        assert personas == {"random_picker"}
