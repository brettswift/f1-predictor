"""Regression tests for the stage-1 media article storage contract."""

from media.storage import finish_fetch_run, start_fetch_run, store_article


def _article(**overrides):
    article = {
    "guid": "feed-guid-1",
    "source": "formula1",
    "url": "https://example.test/article",
    "title": "Original title",
    "body": "A feed summary",
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
    assert db.execute("SELECT COUNT(*) FROM fetch_runs").fetchone()[0] == 0
    indexes = db.execute("PRAGMA index_list(media_articles)").fetchall()
    assert any(index[2] for index in indexes)


def test_store_article_deduplicates_without_rewriting_first_fetch_time(app):
    import app as app_module

    db = app_module.get_db()
    first_id = store_article(db, _article())
    db.execute("UPDATE media_articles SET fetched_at = '2001-01-01 00:00:00' WHERE id = ?", (first_id,))
    second_id = store_article(db, _article())

    row = db.execute("SELECT guid, fetched_at FROM media_articles").fetchone()
    assert first_id == second_id
    assert db.execute("SELECT COUNT(*) FROM media_articles").fetchone()[0] == 1
    assert row["guid"] == "feed-guid-1"
    assert row["fetched_at"] == "2001-01-01 00:00:00"


def test_store_article_updates_changed_title_without_rewriting_fetched_at(app):
    import app as app_module

    db = app_module.get_db()
    article_id = store_article(db, _article())
    db.execute("UPDATE media_articles SET fetched_at = '2001-01-01 00:00:00' WHERE id = ?", (article_id,))
    store_article(db, _article(title="Corrected title"))

    row = db.execute("SELECT title, fetched_at FROM media_articles WHERE id = ?", (article_id,)).fetchone()
    assert row["title"] == "Corrected title"
    assert row["fetched_at"] == "2001-01-01 00:00:00"


def test_store_article_preserves_missing_publication_date_as_null(app):
    import app as app_module

    db = app_module.get_db()
    store_article(db, _article(guid="undated-guid", published_at=None))

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
