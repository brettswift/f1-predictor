"""Regression tests for the stage-1 media article storage contract."""

import hashlib

from media.storage import find_duplicate_by_hash, finish_fetch_run, start_fetch_run, store_article


def _article(**overrides):
    article = {
        "guid": "feed-guid-1",
        "source": "formula1",
        "fetch_url": "https://example.test/article",
        "title": "Original title",
        "content_raw": "A feed summary",
        "published_at": "2026-09-12 10:00:00",
    }
    article.update(overrides)
    return article


def test_media_schema_is_idempotent(app):
    import app as app_module

    app_module.init_db()
    db = app_module.get_db()
    app_module.init_db()

    assert db.execute("SELECT COUNT(*) FROM media_articles").fetchone()[0] == 0
    columns = {row[1] for row in db.execute("PRAGMA table_info(media_articles)").fetchall()}
    assert columns == {
        "guid", "fetch_url", "title", "source", "published_at", "content_raw",
        "content_hash", "fetched_at",
    }


def test_store_article_upserts_and_preserves_fetched_at(app):
    import app as app_module
    import time

    db = app_module.get_db()
    assert store_article(_article(), db=db) is True

    first = db.execute("SELECT guid, title, fetched_at FROM media_articles").fetchone()
    assert db.execute("SELECT COUNT(*) FROM media_articles").fetchone()[0] == 1
    assert first["guid"] == "feed-guid-1"
    assert first["title"] == "Original title"

    # Pause briefly so we can prove fetched_at does not advance.
    time.sleep(0.01)

    assert store_article(_article(title="Corrected title"), db=db) is False

    row = db.execute("SELECT guid, title, fetched_at FROM media_articles").fetchone()
    assert db.execute("SELECT COUNT(*) FROM media_articles").fetchone()[0] == 1
    assert row["guid"] == "feed-guid-1"
    assert row["title"] == "Corrected title"
    assert row["fetched_at"] == first["fetched_at"]


def test_store_article_computes_full_content_sha256_and_finds_duplicate(app):
    import app as app_module

    db = app_module.get_db()
    article = _article(content_raw="Full article text")
    expected_hash = hashlib.sha256(b"Full article text").hexdigest()
    assert store_article(article, db=db) is True

    duplicate = find_duplicate_by_hash(expected_hash, db=db)
    assert duplicate is not None
    assert duplicate["guid"] == article["guid"]
    assert duplicate["content_hash"] == expected_hash
    assert find_duplicate_by_hash("not-a-hash", db=db) is None


def test_store_article_preserves_missing_publication_date_as_null(app):
    import app as app_module

    db = app_module.get_db()
    store_article(_article(guid="undated-guid", published_at=None), db=db)

    assert db.execute(
        "SELECT published_at FROM media_articles WHERE guid = 'undated-guid'"
    ).fetchone()[0] is None


def test_fetch_run_records_source_counts_and_outcome(app):
    import app as app_module

    db = app_module.get_db()
    run_id = start_fetch_run(db, "formula1")
    finish_fetch_run(db, run_id, fetched_count=3, stored_count=2, outcome="success")

    row = db.execute("SELECT source, fetched_count, stored_count, outcome FROM fetch_runs").fetchone()
    assert tuple(row) == ("formula1", 3, 2, "success")
