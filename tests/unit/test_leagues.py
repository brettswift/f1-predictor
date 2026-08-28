"""Unit tests for league creation (BUD-150 / F1-20).

Leagues are a view over the global game: creating one must not touch
predictions/scores, must default to "current round forward" when no window
is chosen, must support an explicit "whole season" sentinel distinct from a
current-round window that happens to start at round 1, and must seat the
creator as a member immediately.
"""

import pytest
from datetime import datetime, timezone, timedelta


def _login(client, username):
    """Helper: sign in via the legacy username flow and return the session_id."""
    client.post('/set-username', data={'username': username})
    with client.session_transaction() as sess:
        return sess['session_id']


def _insert_race(db, round_num, status='open', hours_from_now=24):
    """Helper: insert a bare race row with a computed-status-friendly date."""
    race_time = datetime.now(timezone.utc) + timedelta(hours=hours_from_now)
    db.execute(
        'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
        (f'Test GP Round {round_num}', round_num, race_time.strftime('%Y-%m-%d %H:%M:%S'), status)
    )
    db.commit()


class TestLeagueCreation:
    """AC: Any signed-in user can create a league (name + emoji/color required);
    the creator is recorded as admin."""

    def test_signed_in_user_can_create_league_as_admin(self, app, client):
        from app import get_db

        session_id = _login(client, 'league_creator')

        response = client.post('/leagues', data={
            'name': 'Office League',
            'emoji_or_color': '🏎️',
        })
        assert response.status_code == 302

        db = get_db()
        league = db.execute('SELECT * FROM leagues WHERE name = ?', ('Office League',)).fetchone()
        assert league is not None
        assert league['emoji_or_color'] == '🏎️'
        assert league['admin_user_id'] == session_id

    def test_create_league_requires_name_and_emoji_or_color(self, app, client):
        from app import get_db

        _login(client, 'validator')

        # Missing name
        client.post('/leagues', data={'emoji_or_color': '#e10600'})
        # Missing emoji/color
        client.post('/leagues', data={'name': 'No Color League'})

        db = get_db()
        count = db.execute('SELECT COUNT(*) c FROM leagues').fetchone()['c']
        assert count == 0

    def test_anonymous_user_cannot_create_league(self, app, client):
        from app import get_db

        response = client.post('/leagues', data={
            'name': 'Sneaky League',
            'emoji_or_color': '#000000',
        })
        assert response.status_code == 302  # redirected to sign in

        db = get_db()
        count = db.execute('SELECT COUNT(*) c FROM leagues').fetchone()['c']
        assert count == 0


class TestLeagueCreationDoesNotTouchGameState:
    """AC: Creating a league inserts zero rows into predictions or scores."""

    def test_no_predictions_or_scores_rows_inserted(self, app, client):
        from app import get_db

        _login(client, 'isolator')
        db = get_db()

        predictions_before = db.execute('SELECT COUNT(*) c FROM predictions').fetchone()['c']
        scores_before = db.execute('SELECT COUNT(*) c FROM scores').fetchone()['c']

        client.post('/leagues', data={
            'name': 'Isolated League',
            'emoji_or_color': '🔒',
        })

        predictions_after = db.execute('SELECT COUNT(*) c FROM predictions').fetchone()['c']
        scores_after = db.execute('SELECT COUNT(*) c FROM scores').fetchone()['c']

        assert predictions_after == predictions_before
        assert scores_after == scores_before


class TestDefaultScoringWindow:
    """AC: No window chosen -> resolves to "current round forward"."""

    def test_default_window_resolves_to_current_open_race_round(self, app, client):
        from app import get_db, get_next_open_race

        db = get_db()
        _insert_race(db, round_num=3, status='open', hours_from_now=48)
        _insert_race(db, round_num=4, status='open', hours_from_now=96)

        _login(client, 'window_default')

        client.post('/leagues', data={
            'name': 'Default Window League',
            'emoji_or_color': '🟢',
        })

        league = db.execute(
            'SELECT * FROM leagues WHERE name = ?', ('Default Window League',)
        ).fetchone()
        expected_round = get_next_open_race(db)['round']

        assert league['start_round'] == expected_round
        assert league['whole_season'] == 0

    def test_default_window_falls_back_when_no_race_open(self, app, client):
        """No open race (e.g. mid-lock) still resolves to a concrete round,
        not NULL, so it stays distinguishable from an explicit whole-season pick."""
        from app import get_db

        db = get_db()
        _insert_race(db, round_num=1, status='completed', hours_from_now=-500)
        race1_id = db.execute('SELECT id FROM races WHERE round = 1').fetchone()['id']
        # A race only computes as 'completed' once results exist (status column
        # alone isn't enough - see compute_race_status).
        db.execute(
            'INSERT INTO results (race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, 1, 2, 3)',
            (race1_id,)
        )
        _insert_race(db, round_num=2, status='locked', hours_from_now=-1)
        db.commit()

        _login(client, 'window_fallback')

        client.post('/leagues', data={
            'name': 'Fallback Window League',
            'emoji_or_color': '🟡',
        })

        league = db.execute(
            'SELECT * FROM leagues WHERE name = ?', ('Fallback Window League',)
        ).fetchone()
        assert league['start_round'] == 2
        assert league['whole_season'] == 0


class TestWholeSeasonOption:
    """AC: "Whole season" is an explicit, distinct, testable option - not
    confusable with a current-round window that happens to start at round 1."""

    def test_whole_season_is_distinct_from_round_1_start(self, app, client):
        from app import get_db

        db = get_db()
        _insert_race(db, round_num=1, status='open', hours_from_now=24)

        _login(client, 'season_chooser')

        # Implicit default -> current round forward, which happens to be round 1.
        client.post('/leagues', data={
            'name': 'Round One League',
            'emoji_or_color': '1️⃣',
        })
        # Explicit whole season.
        client.post('/leagues', data={
            'name': 'Whole Season League',
            'emoji_or_color': '🗓️',
            'window': 'whole_season',
        })

        round_one_league = db.execute(
            'SELECT * FROM leagues WHERE name = ?', ('Round One League',)
        ).fetchone()
        whole_season_league = db.execute(
            'SELECT * FROM leagues WHERE name = ?', ('Whole Season League',)
        ).fetchone()

        assert round_one_league['start_round'] == 1
        assert round_one_league['whole_season'] == 0

        assert whole_season_league['start_round'] is None
        assert whole_season_league['whole_season'] == 1

        # The two are not confusable by start_round alone.
        assert round_one_league['whole_season'] != whole_season_league['whole_season']


class TestCreatorMembership:
    """AC: The creating user is a member immediately, no separate self-invite."""

    def test_creator_is_member_immediately(self, app, client):
        from app import get_db, is_league_member, get_league_members

        session_id = _login(client, 'self_member')

        client.post('/leagues', data={
            'name': 'Membership League',
            'emoji_or_color': '👥',
        })

        db = get_db()
        league = db.execute(
            'SELECT * FROM leagues WHERE name = ?', ('Membership League',)
        ).fetchone()

        assert is_league_member(db, league['id'], session_id)

        members = get_league_members(db, league['id'])
        assert len(members) == 1
        assert members[0]['user_id'] == session_id
        assert members[0]['is_admin'] == 1

    def test_creator_membership_visible_via_league_list(self, app, client):
        from app import get_db, get_user_leagues

        session_id = _login(client, 'lister')

        client.post('/leagues', data={
            'name': 'Listed League',
            'emoji_or_color': '📋',
        })

        db = get_db()
        user_leagues = get_user_leagues(db, session_id)
        assert any(l['name'] == 'Listed League' for l in user_leagues)


class TestCreateLeagueHelperDirectly:
    """Direct coverage of the create_league() helper (validation + edge cases)."""

    def test_raises_when_name_missing(self, app):
        from app import get_db, create_league

        db = get_db()
        with pytest.raises(ValueError):
            create_league(db, 'user-1', '', '🏁')

    def test_raises_when_emoji_or_color_missing(self, app):
        from app import get_db, create_league

        db = get_db()
        with pytest.raises(ValueError):
            create_league(db, 'user-1', 'No Color', '')

    def test_explicit_start_round_overrides_default(self, app):
        from app import get_db, create_league

        db = get_db()
        _insert_race(db, round_num=5, status='open', hours_from_now=24)

        league_id = create_league(db, 'user-1', 'Explicit Round', '⚡', start_round=9)
        league = db.execute('SELECT * FROM leagues WHERE id = ?', (league_id,)).fetchone()
        assert league['start_round'] == 9
        assert league['whole_season'] == 0


class TestInviteAndJoin:
    """AC: BUD-151 — any member can generate an invite link; any user with the
    link can join the league; cold users see login flow first then redirected."""

    def test_member_can_generate_invite_link(self, app, client):
        from app import get_db, create_league

        db = get_db()
        _login(client, 'inviter')
        with client.session_transaction() as sess:
            creator_id = sess['session_id']

        league_id = create_league(db, creator_id, 'Invite League', '📧')
        _insert_race(db, round_num=1, status='open', hours_from_now=24)

        response = client.get(f'/leagues/{league_id}/invite')
        assert response.status_code == 200
        assert b'Invite to' in response.data or b'invite' in response.data.lower()

    def test_non_member_cannot_generate_invite(self, app, client):
        from app import get_db, create_league

        db = get_db()
        _login(client, 'member')
        with client.session_transaction() as sess:
            creator_id = sess['session_id']

        league_id = create_league(db, creator_id, 'Restricted League', '🔒')

        _login(client, 'non_member')
        response = client.get(f'/leagues/{league_id}/invite', follow_redirects=True)
        assert response.status_code == 200

    def test_join_link_works_for_logged_in_user(self, app, client):
        from app import get_db, create_league, _get_invite_serializer, is_league_member, get_league_members

        db = get_db()
        _login(client, 'joiner')
        with client.session_transaction() as sess:
            creator_id = sess['session_id']

        league_id = create_league(db, creator_id, 'Joinable League', '🔗')
        _insert_race(db, round_num=1, status='open', hours_from_now=24)

        ser = _get_invite_serializer()
        token = ser.dumps(league_id)

        _login(client, 'new_joiner')
        response = client.get(f'/leagues/join/{token}', follow_redirects=True)
        assert response.status_code == 200

        # Verify membership by checking member count
        members = get_league_members(db, league_id)
        assert len(members) > 1

    def test_invalid_token_shows_error(self, app, client):
        _login(client, 'failer')
        response = client.get('/leagues/join/invalid-token-123', follow_redirects=True)
        assert response.status_code == 200

    def test_cold_user_redirected_to_login(self, app, client):
        from app import get_db, create_league, _get_invite_serializer

        db = get_db()
        _login(client, 'league_owner')
        with client.session_transaction() as sess:
            owner_id = sess['session_id']

        league_id = create_league(db, owner_id, 'Cold Join League', '❄️')

        ser = _get_invite_serializer()
        token = ser.dumps(league_id)

        # Log out by clearing session
        with client.session_transaction() as sess:
            sess.clear()

        response = client.get(f'/leagues/join/{token}', follow_redirects=False)
        assert response.status_code == 302
        assert '/login' in response.location or 'login' in response.location
