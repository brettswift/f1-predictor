"""SQLite storage shared by every media-feed adapter.

This module deliberately knows nothing about a scheduler, Kubernetes, or a
cloud provider. Feed adapters supply a source-agnostic article mapping and the
fetch runner records its own lifecycle with the helpers below.
"""

from collections.abc import Mapping
import hashlib
import sqlite3
from typing import Optional


_REQUIRED_FIELDS = ("guid", "fetch_url", "title", "source", "content_raw")


def _connection(db: sqlite3.Connection | None) -> sqlite3.Connection:
    if db is not None:
        return db
    # Import lazily so feed workers can also supply an explicit connection.
    from app import get_db
    return get_db()


def _normalise_article(article: Mapping[str, object]) -> dict[str, object]:
    """Accept the storage contract, with legacy adapter aliases during rollout."""
    values = dict(article)
    values.setdefault("fetch_url", values.get("url"))
    values.setdefault("content_raw", values.get("body"))
    missing = [field for field in _REQUIRED_FIELDS if not values.get(field)]
    if missing:
        raise ValueError(f"article is missing required field(s): {', '.join(missing)}")
    values["content_hash"] = hashlib.sha256(
        str(values["content_raw"]).encode("utf-8")
    ).hexdigest()
    return values


def store_article(article: Mapping[str, object], *, db: sqlite3.Connection | None = None) -> bool:
    """Insert an article once, returning whether this call created the row.

    The feed GUID is the physical dedup boundary. ``INSERT OR IGNORE`` makes
    re-polling safe even when multiple workers receive the same feed item.
    """
    values = _normalise_article(article)
    cursor = _connection(db).execute(
        """
        INSERT OR IGNORE INTO media_articles
            (guid, fetch_url, title, source, published_at, content_raw, content_hash)
        VALUES
            (:guid, :fetch_url, :title, :source, :published_at, :content_raw, :content_hash)
        """,
        values,
    )
    return cursor.rowcount == 1


def find_duplicate_by_hash(
    content_hash: str, *, db: sqlite3.Connection | None = None
) -> Optional[dict[str, object]]:
    """Return the first stored article with this full-text SHA-256, if any."""
    row = _connection(db).execute(
        "SELECT * FROM media_articles WHERE content_hash = ? LIMIT 1", (content_hash,)
    ).fetchone()
    return dict(row) if row is not None else None


def start_fetch_run(db: sqlite3.Connection, source: str) -> int:
    """Record the beginning of one source poll and return its run id."""
    cursor = db.execute("INSERT INTO fetch_runs (source) VALUES (?)", (source,))
    return cursor.lastrowid


def finish_fetch_run(
    db: sqlite3.Connection,
    run_id: int,
    *,
    fetched_count: int,
    stored_count: int,
    outcome: str,
    error_message: str | None = None,
) -> None:
    """Finalize a fetch run with its outcome and article counts."""
    if outcome not in {"success", "failed"}:
        raise ValueError("outcome must be 'success' or 'failed'")
    db.execute(
        """
        UPDATE fetch_runs
        SET finished_at = CURRENT_TIMESTAMP,
            fetched_count = ?,
            stored_count = ?,
            outcome = ?,
            error_message = ?
        WHERE id = ?
        """,
        (fetched_count, stored_count, outcome, error_message, run_id),
    )
