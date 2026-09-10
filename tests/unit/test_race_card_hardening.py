"""BUD-187: harden personal race-card read model and separate unavailable from no vote."""
from datetime import datetime, timezone, timedelta
import re
import pytest

import app as app_module


def _seed_open_race(db):
    """Insert an open race and three drivers, return race_id."""
    when = (datetime.now(timezone.utc) + timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S')
    db.execute(
        "INSERT INTO races (name, round, date, status) VALUES ('BUD187 GP', 987, ?, 'open')",
        (when,),
    )
    race_id = db.execute('SELECT id FROM races WHERE round=987').fetchone()['id']
    for i, name in enumerate(['Alfa Driver', 'Bravo Driver', 'Charlie Driver'], 987):
        db.execute(
            'INSERT INTO drivers (id, driver_id, name, number, code) VALUES (?, ?, ?, ?, ?)',
            (i, str(i), name, i, name.split()[0][:3].upper()),
        )
    db.commit()
    return race_id


def _create_user(client, db, username):
    """Create a user and return its session_id."""
    client.post('/set-username', data={'username': username})
    return db.execute('SELECT session_id FROM users WHERE username = ?', (username,)).fetchone()['session_id']


class TestBuildDeskReadStates:
    """build_desk must distinguish no-vote, no-sc, unavailable and saved."""

    def test_no_prediction_no_sc_shows_not_voted(self, app, client):
        from app import get_db, build_desk
        db = get_db()
        race_id = _seed_open_race(db)
        sid = _create_user(client, db, 'bud187-not-voted')
        user = db.execute('SELECT * FROM users WHERE session_id = ?', (sid,)).fetchone()
        desk = build_desk(db, user)
        assert desk['user_prediction'] is None
        assert desk['sc_vote'] is None
        assert desk['prediction_error'] is False
        assert desk['sc_error'] is False
        # The desk still renders a votable card.
        page = client.get('/home').get_data(as_text=True)
        assert '<span class="tag warn">Not voted</span>' in page

    def test_saved_prediction_and_call_present(self, app, client):
        from app import get_db, build_desk
        db = get_db()
        race_id = _seed_open_race(db)
        sid = _create_user(client, db, 'bud187-saved')
        db.execute(
            'INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?, ?)',
            (sid, race_id, 987, 988, 989),
        )
        db.execute(
            'INSERT INTO sc_votes (user_id, race_id, conviction, multiplier) VALUES (?, ?, ?, ?)',
            (sid, race_id, 42, 1.7),
        )
        db.commit()
        user = db.execute('SELECT * FROM users WHERE session_id = ?', (sid,)).fetchone()
        desk = build_desk(db, user)
        assert desk['user_prediction'] is not None
        assert desk['user_prediction']['p1_driver_id'] == 987
        assert desk['sc_vote'] is not None
        assert desk['sc_vote']['conviction'] == 42
        assert desk['sc_vote']['multiplier'] == 1.7
        assert desk['prediction_error'] is False
        assert desk['sc_error'] is False

    def test_legacy_missing_sc_row_is_none(self, app, client):
        """A prediction without an SC row must be distinguishable from an error."""
        from app import get_db, build_desk
        db = get_db()
        race_id = _seed_open_race(db)
        sid = _create_user(client, db, 'bud187-legacy')
        db.execute(
            'INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?, ?)',
            (sid, race_id, 987, 988, 989),
        )
        db.commit()
        user = db.execute('SELECT * FROM users WHERE session_id = ?', (sid,)).fetchone()
        desk = build_desk(db, user)
        assert desk['user_prediction'] is not None
        assert desk['sc_vote'] is None
        assert desk['sc_error'] is False

    def test_db_read_failure_surfaces_error_flags(self, app, client):
        from app import build_desk

        class _FailingReadsDB:
            """Fail only the two per-user reads, so build_desk still resolves next_race."""

            def __init__(self, real_db):
                self._real = real_db

            def execute(self, sql, params=None):
                sql_l = sql.strip().lower()
                if 'from predictions' in sql_l or 'from sc_votes' in sql_l:
                    raise RuntimeError('database is unavailable')
                if params is not None:
                    return self._real.execute(sql, params)
                return self._real.execute(sql)

        db = app_module.get_db()
        _ = _seed_open_race(db)
        sid = _create_user(client, db, 'bud187-err')
        user = db.execute('SELECT * FROM users WHERE session_id = ?', (sid,)).fetchone()
        desk = build_desk(_FailingReadsDB(db), user)
        assert desk['prediction_error'] is True
        assert desk['sc_error'] is True


class TestSavedCardUsesStoredValues:
    """Read-only saved card must use authoritative stored conviction/multiplier."""

    def test_saved_multiplier_does_not_reprice_on_crowd_shift(self, app, client):
        """Place a call while consensus is 0, then change the crowd drastically:
        the stored multiplier must stay unchanged and the desk must render it."""
        from app import get_db, sc_multiplier
        db = get_db()
        race_id = _seed_open_race(db)
        sid = _create_user(client, db, 'bud187-reprice')

        # Submit a positive conviction when no one else has voted.
        response = client.post(
            f'/predict/{race_id}',
            data={'p1': 987, 'p2': 988, 'p3': 989, 'sc_conviction': 80},
            follow_redirects=True,
        )
        assert response.status_code == 200

        stored = db.execute(
            'SELECT * FROM sc_votes WHERE user_id = ? AND race_id = ?', (sid, race_id)
        ).fetchone()
        expected_multiplier = stored['multiplier']
        assert stored['conviction'] == 80
        assert stored['multiplier'] == sc_multiplier(80, 0)

        # Now simulate a later crowd swing (many other players voting negative).
        for i in range(10):
            other_sid = f'bud187-other-{i}'
            db.execute(
                'INSERT OR IGNORE INTO users (session_id, username) VALUES (?, ?)',
                (other_sid, f'other{i}'),
            )
            db.execute(
                'INSERT INTO sc_votes (user_id, race_id, conviction, multiplier) VALUES (?, ?, ?, ?)',
                (other_sid, race_id, -100, 1.0),
            )
        db.commit()

        # Re-read the desk after the crowd changed.
        from app import build_desk
        user = db.execute('SELECT * FROM users WHERE session_id = ?', (sid,)).fetchone()
        desk = build_desk(db, user)
        assert desk['sc_vote']['multiplier'] == expected_multiplier
        assert desk['sc_vote']['conviction'] == 80

        page = client.get('/home').get_data(as_text=True)
        # The hidden saved multiplier rendered in the track must match the stored value.
        assert f'data-saved-multiplier="{expected_multiplier}"' in page


class TestAtomicWriteAndDuplicatePost:
    """Prediction + SC write is atomic and duplicate submissions cannot alter a locked vote."""

    def test_zero_abstention_creates_sc_row(self, app, client):
        """Explicit 0 conviction must still create an sc_votes row (legacy migration safety)."""
        from app import get_db
        db = get_db()
        race_id = _seed_open_race(db)
        sid = _create_user(client, db, 'bud187-zero')
        response = client.post(
            f'/predict/{race_id}',
            data={'p1': 987, 'p2': 988, 'p3': 989, 'sc_conviction': 0},
            follow_redirects=True,
        )
        assert response.status_code == 200
        row = db.execute(
            'SELECT * FROM sc_votes WHERE user_id = ? AND race_id = ?', (sid, race_id)
        ).fetchone()
        assert row is not None
        assert row['conviction'] == 0
        assert row['multiplier'] == 1.0

    def test_duplicate_post_does_not_overwrite_existing_prediction(self, app, client):
        from app import get_db
        db = get_db()
        race_id = _seed_open_race(db)
        sid = _create_user(client, db, 'bud187-dup')
        response = client.post(
            f'/predict/{race_id}',
            data={'p1': 987, 'p2': 988, 'p3': 989, 'sc_conviction': 40},
            follow_redirects=True,
        )
        assert response.status_code == 200
        assert b'Card locked in' in response.data

        # A second post must be rejected and must not mutate the saved picks.
        response2 = client.post(
            f'/predict/{race_id}',
            data={'p1': 989, 'p2': 988, 'p3': 987, 'sc_conviction': -40},
            follow_redirects=True,
        )
        assert response2.status_code == 200
        assert b'already submitted predictions' in response2.data or b'your picks are safe' in response2.data

        pred = db.execute(
            'SELECT * FROM predictions WHERE user_id = ? AND race_id = ?', (sid, race_id)
        ).fetchone()
        assert pred['p1_driver_id'] == 987
        sc = db.execute(
            'SELECT * FROM sc_votes WHERE user_id = ? AND race_id = ?', (sid, race_id)
        ).fetchone()
        assert sc['conviction'] == 40

    def test_race_lock_blocks_overwrite(self, app, client):
        """Once a race is locked, the POST guard refuses further submissions."""
        from app import get_db
        db = get_db()
        race_id = _seed_open_race(db)
        sid = _create_user(client, db, 'bud187-locked')
        client.post(
            f'/predict/{race_id}',
            data={'p1': 987, 'p2': 988, 'p3': 989, 'sc_conviction': 30},
            follow_redirects=True,
        )
        db.execute("UPDATE races SET status = 'locked' WHERE id = ?", (race_id,))
        db.commit()
        response = client.post(
            f'/predict/{race_id}',
            data={'p1': 989, 'p2': 988, 'p3': 987, 'sc_conviction': -30},
            follow_redirects=True,
        )
        assert response.status_code == 200
        page = response.get_data(as_text=True)
        assert 'locked' in page.lower() or 'already submitted' in page.lower()
        pred = db.execute(
            'SELECT * FROM predictions WHERE user_id = ? AND race_id = ?', (sid, race_id)
        ).fetchone()
        assert pred['p1_driver_id'] == 987


class TestMultiAccountIsolation:
    """Race-card data must not leak across users."""

    def test_other_users_card_is_not_my_card(self, app, client):
        from app import get_db
        db = get_db()
        race_id = _seed_open_race(db)
        alice_sid = _create_user(client, db, 'bud187-alice')
        client.post(
            f'/predict/{race_id}',
            data={'p1': 987, 'p2': 988, 'p3': 989, 'sc_conviction': 55},
            follow_redirects=True,
        )

        # Switch to bob.
        with client.session_transaction() as sess:
            sess.clear()
        bob_sid = _create_user(client, db, 'bud187-bob')
        from app import build_desk
        user = db.execute('SELECT * FROM users WHERE session_id = ?', (bob_sid,)).fetchone()
        desk = build_desk(db, user)
        assert desk['user_prediction'] is None
        assert desk['sc_vote'] is None
        assert desk['prediction_error'] is False
        assert desk['sc_error'] is False


class TestErrorStatesForDistribution:
    """get_pick_distribution and get_sc_crowd must not claim empty on failure."""

    def test_pick_distribution_failure_is_unavailable(self, app):
        result = app_module.get_pick_distribution(_CrashDB(), race_id=1)
        assert result['status'] == 'unavailable'
        assert result['total'] == 0
        assert result['rows'] == []

    def test_sc_crowd_failure_is_unavailable(self, app):
        result = app_module.get_sc_crowd(_CrashDB(), race_id=1)
        assert result['status'] == 'unavailable'
        assert result['votes'] == 0


class _CrashDB:
    def execute(self, *args, **kwargs):
        raise RuntimeError('database is unavailable')
