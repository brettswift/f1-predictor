#!/usr/bin/env python3
"""Season replay harness for F1-110.

Populates the leaderboard with deterministic synthetic users, predictions, and
scores by calling the production calculate_score() path. Synthetic data is
tagged via users.is_synthetic (F1-111) so it can be torn down cleanly.

Usage:
    python3 -m scripts.replay_season --season 2026 --seed 42
    python3 -m scripts.replay_season --teardown --yes
"""

from __future__ import annotations

import argparse
import os
import random
import sqlite3
import sys
import uuid
from pathlib import Path
from typing import Iterable

# Resolve project root relative to this script (scripts/replay_season.py).
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

import app as app_module

DEFAULT_SEASON = 2026
DEFAULT_SYNTHETIC_USER_COUNT = 25
MIN_SCORED_ROUNDS = 10


class ReplayError(Exception):
    """Raised when replay preconditions are not met."""


def _db_path() -> str:
    return os.environ.get("DATABASE_PATH", app_module.app.config["DATABASE"])


def _connect() -> sqlite3.Connection:
    db = sqlite3.connect(_db_path())
    db.row_factory = sqlite3.Row
    return db


def _synthetic_uuid(season: int, seed: int, index: int) -> str:
    """Deterministic UUID for a synthetic user."""
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"f1-replay:{season}:{seed}:{index}"))


def _create_synthetic_users(
    db: sqlite3.Connection, season: int, seed: int, count: int
) -> list[tuple[str, str]]:
    """Create deterministic synthetic users.

    Returns a list of (session_id, username) tuples in creation order.
    """
    users: list[tuple[str, str]] = []
    for i in range(count):
        username = f"synthetic_{i + 1:03d}"
        session_id = _synthetic_uuid(season, seed, i)
        db.execute(
            "INSERT OR IGNORE INTO users (session_id, username, is_synthetic) VALUES (?, ?, 1)",
            (session_id, username),
        )
        users.append((session_id, username))
    db.commit()
    return users


def _delete_synthetic_users(db: sqlite3.Connection) -> tuple[int, int, int]:
    """Remove all synthetic users and their predictions/scores.

    Returns (deleted_users, deleted_predictions, deleted_scores).
    """
    synthetic_ids = [
        row["session_id"]
        for row in db.execute("SELECT session_id FROM users WHERE is_synthetic = 1").fetchall()
    ]
    if not synthetic_ids:
        return 0, 0, 0

    placeholders = ",".join("?" * len(synthetic_ids))
    params = tuple(synthetic_ids)

    cur = db.execute(f"DELETE FROM scores WHERE user_id IN ({placeholders})", params)
    scores_deleted = cur.rowcount
    cur = db.execute(f"DELETE FROM predictions WHERE user_id IN ({placeholders})", params)
    predictions_deleted = cur.rowcount
    cur = db.execute(f"DELETE FROM users WHERE session_id IN ({placeholders})", params)
    users_deleted = cur.rowcount
    db.commit()
    return users_deleted, predictions_deleted, scores_deleted


def _scored_races(db: sqlite3.Connection, season: int) -> list[sqlite3.Row]:
    """Return completed races with results for the season, ordered by round."""
    return db.execute(
        """
        SELECT r.id, r.name, r.round, r.date,
               res.p1_driver_id, res.p2_driver_id, res.p3_driver_id
        FROM races r
        JOIN results res ON r.id = res.race_id
        WHERE strftime('%Y', r.date) = ?
        ORDER BY r.round ASC
    """,
        (str(season),),
    ).fetchall()


def _all_driver_ids(db: sqlite3.Connection) -> list[int]:
    return [row["id"] for row in db.execute("SELECT id FROM drivers ORDER BY id").fetchall()]


def _generate_prediction(driver_ids: list[int], rng: random.Random) -> tuple[int, int, int]:
    """Pick three distinct drivers for P1/P2/P3."""
    p1, p2, p3 = rng.sample(driver_ids, 3)
    return p1, p2, p3


def _ensure_predictions(
    db: sqlite3.Connection,
    users: list[tuple[str, str]],
    races: list[sqlite3.Row],
    driver_ids: list[int],
    seed: int,
) -> None:
    """Generate deterministic synthetic predictions through the live schema."""
    rng = random.Random(seed)
    for race in races:
        for user_id, _username in users:
            existing = db.execute(
                "SELECT 1 FROM predictions WHERE user_id = ? AND race_id = ?",
                (user_id, race["id"]),
            ).fetchone()
            if existing:
                continue
            p1, p2, p3 = _generate_prediction(driver_ids, rng)
            db.execute(
                """
                INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
                VALUES (?, ?, ?, ?, ?)
            """,
                (user_id, race["id"], p1, p2, p3),
            )
    db.commit()


def _calculate_and_insert_scores(
    db: sqlite3.Connection, users: list[tuple[str, str]], races: list[sqlite3.Row]
) -> None:
    """Score every synthetic prediction using app.calculate_score()."""
    for race in races:
        result = {
            "p1_driver_id": race["p1_driver_id"],
            "p2_driver_id": race["p2_driver_id"],
            "p3_driver_id": race["p3_driver_id"],
        }
        for user_id, _username in users:
            pred = db.execute(
                """
                SELECT p1_driver_id, p2_driver_id, p3_driver_id
                FROM predictions WHERE user_id = ? AND race_id = ?
            """,
                (user_id, race["id"]),
            ).fetchone()
            if pred is None:
                continue
            points = app_module.calculate_score(dict(pred), result)
            db.execute(
                """
                INSERT INTO scores (user_id, race_id, points)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id, race_id) DO UPDATE SET points = excluded.points
            """,
                (user_id, race["id"], points),
            )
    db.commit()


def _leaderboard(
    db: sqlite3.Connection, season: int, user_ids: Iterable[str] | None = None
) -> list[sqlite3.Row]:
    """Return a deterministic leaderboard (score desc, username asc tie-break)."""
    if user_ids is not None:
        user_list = list(user_ids)
        placeholders = ",".join("?" for _ in user_list)
        return db.execute(
            f"""
            SELECT u.username, COALESCE(SUM(s.points), 0) as total_score
            FROM users u
            LEFT JOIN scores s ON u.session_id = s.user_id
            LEFT JOIN races r ON s.race_id = r.id AND strftime('%Y', r.date) = ?
            WHERE u.session_id IN ({placeholders})
            GROUP BY u.session_id
            ORDER BY total_score DESC, u.username ASC
        """,
            (str(season),) + tuple(user_list),
        ).fetchall()

    return db.execute(
        """
        SELECT u.username, COALESCE(SUM(s.points), 0) as total_score
        FROM users u
        LEFT JOIN scores s ON u.session_id = s.user_id
        LEFT JOIN races r ON s.race_id = r.id AND strftime('%Y', r.date) = ?
        GROUP BY u.session_id
        ORDER BY total_score DESC, u.username ASC
    """,
        (str(season),),
    ).fetchall()


def _format_leaderboard(rows: list[sqlite3.Row]) -> str:
    lines = []
    lines.append(f"{'Rank':<5} {'User':<20} {'Score':<6}")
    lines.append("-" * 35)
    for i, row in enumerate(rows, 1):
        lines.append(f"{i:<5} {row['username']:<20} {row['total_score']:<6}")
    return "\n".join(lines)


def _count_table(db: sqlite3.Connection, table: str) -> int:
    return db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _snapshot_counts(db: sqlite3.Connection) -> dict[str, int]:
    return {
        "users": _count_table(db, "users"),
        "predictions": _count_table(db, "predictions"),
        "scores": _count_table(db, "scores"),
    }


def run_replay(
    season: int,
    seed: int,
    db: sqlite3.Connection | None = None,
    user_count: int = DEFAULT_SYNTHETIC_USER_COUNT,
) -> dict:
    """Run the full replay and return result metadata.

    If ``db`` is provided it is used directly and left open; otherwise a new
    connection is opened and closed.
    """
    close_db = db is None
    db = db or _connect()

    try:
        counts_before = _snapshot_counts(db)

        driver_ids = _all_driver_ids(db)
        if len(driver_ids) < 3:
            raise ReplayError(f"Need at least 3 drivers, found {len(driver_ids)}")

        races = _scored_races(db, season)
        if len(races) < MIN_SCORED_ROUNDS:
            raise ReplayError(
                f"Need at least {MIN_SCORED_ROUNDS} scored races for season {season}, "
                f"found {len(races)}"
            )

        users = _create_synthetic_users(db, season, seed, user_count)
        _ensure_predictions(db, users, races, driver_ids, seed)
        _calculate_and_insert_scores(db, users, races)
        counts_after = _snapshot_counts(db)
        leaderboard = _leaderboard(db, season, (u[0] for u in users))
    finally:
        if close_db:
            db.close()

    return {
        "users": users,
        "races": races,
        "leaderboard": leaderboard,
        "counts_before": counts_before,
        "counts_after": counts_after,
    }


def run_teardown(db: sqlite3.Connection | None = None) -> dict:
    """Purge all replay-created data and return deletion metadata."""
    close_db = db is None
    db = db or _connect()

    try:
        counts_before = _snapshot_counts(db)
        users_deleted, predictions_deleted, scores_deleted = _delete_synthetic_users(db)
        counts_after = _snapshot_counts(db)
    finally:
        if close_db:
            db.close()

    return {
        "deleted_users": users_deleted,
        "deleted_predictions": predictions_deleted,
        "deleted_scores": scores_deleted,
        "counts_before": counts_before,
        "counts_after": counts_after,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="F1 season replay harness")
    parser.add_argument("--season", type=int, default=DEFAULT_SEASON)
    parser.add_argument("--seed", type=int, required=False)
    parser.add_argument("--teardown", action="store_true")
    parser.add_argument("--db", type=str, default=None, help="Database path override")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompts")
    args = parser.parse_args(argv)

    if args.db:
        os.environ["DATABASE_PATH"] = args.db
        app_module.app.config["DATABASE"] = args.db

    if args.teardown:
        if not args.yes:
            try:
                confirm = input("Delete all synthetic replay data? [y/N]: ")
            except (EOFError, KeyboardInterrupt):
                confirm = "n"
            if confirm.lower() != "y":
                print("Teardown cancelled.")
                return 0
        result = run_teardown()
        print(
            f"Teardown complete: {result['deleted_users']} users, "
            f"{result['deleted_predictions']} predictions, {result['deleted_scores']} scores removed."
        )
        return 0

    if args.seed is None:
        print("error: --seed is required for replay", file=sys.stderr)
        return 1

    result = run_replay(args.season, args.seed)
    print(_format_leaderboard(result["leaderboard"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
