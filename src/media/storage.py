"""SQLite storage shared by every media-feed adapter.

This module deliberately knows nothing about a scheduler, Kubernetes, or a
cloud provider. Feed adapters supply a source-agnostic article mapping and the
fetch runner records its own lifecycle with the helpers below.
"""

from collections.abc import Mapping
import sqlite3


_ARTICLE_FIELDS = ("guid", "source", "url", "title", "body", "published_at")


def store_article(db: sqlite3.Connection, article: Mapping[str, object]) -> int:
    """Insert or refresh one feed article and return its database id.

    A feed GUID is immutable identity. Re-fetching it updates mutable article
    data (for example a corrected title) but intentionally never rewrites
    ``fetched_at``: that timestamp records first discovery, not last polling.
    ``published_at`` is passed through unchanged, including ``None``.
    """
    missing = [field for field in _ARTICLE_FIELDS[:4] if field not in article]
    if missing:
        raise ValueError(f"article is missing required field(s): {', '.join(missing)}")

    values = {field: article.get(field) for field in _ARTICLE_FIELDS}
    row = db.execute(
        """
        INSERT INTO media_articles (guid, source, url, title, body, published_at)
        VALUES (:guid, :source, :url, :title, :body, :published_at)
        ON CONFLICT(guid) DO UPDATE SET
            source = excluded.source,
            url = excluded.url,
            title = excluded.title,
            body = excluded.body,
            published_at = excluded.published_at
        RETURNING id
        """,
        values,
    ).fetchone()
    return row[0]


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
