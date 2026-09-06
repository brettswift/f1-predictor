"""BUD-155 / F1-25: multi-league membership.

Epic E3, Phase 1 direct proof of the §3.5 invariant: "you can belong to
many leagues at once ... one pick counts everywhere." Extends the F1-22
fixture (BUD-152, `test_league_standings_pure_filter.py`) from 2 leagues to
3, and adds the leave/removal path (F1-26) that F1-22 didn't need to cover.

Acceptance criteria covered (see Linear BUD-155):
  AC1  A user can hold membership rows in N>=2 leagues at once - here N=3.
  AC2  One `predictions` row for a user+race contributes identically to
       every league that user belongs to for that race, and to the global
       leaderboard, with no duplicate `predictions`/`scores` rows created
       per league - sourced from a single `scores` row.
  AC3  A league switcher (`/leagues`, and the `/home` league-select form)
       lists all of a user's leagues; navigating between them reuses the
       same session (no re-auth) and the same prediction (no re-entry).
  AC4  Leaving one league does not change the user's standing or
       membership in their other leagues, nor the global leaderboard.
"""

import re

import pytest

from app import get_db, get_standings, get_user_leagues, remove_league_member

RACE_ID = 1
RACE_DATE = '2026-03-08 14:00:00'

MULTI = 'bud155-multi'      # member of all 3 leagues, submits one prediction
NONMEMBER = 'bud155-outside'  # has points, belongs to no league

DRIVERS = (1, 2, 3, 4, 5, 6)
PICK = (1, 2, 3)
POINTS = 20  # exact podium: 10 + 6 + 4

LEAGUE_NAMES = ('BUD155 Alpha', 'BUD155 Bravo', 'BUD155 Charlie')


def _username(session_id):
    return session_id.split('-')[-1]


def _login(client, session_id):
    with client.session_transaction() as sess:
        sess['session_id'] = session_id


def _insert_race(db):
    db.execute(
        'INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
        (RACE_ID, 'BUD155 GP', 1, RACE_DATE, 'completed'))


def _insert_drivers(db):
    for driver_id, number in enumerate(DRIVERS, start=1):
        db.execute(
            'INSERT INTO drivers (id, driver_id, name, number, code) '
            'VALUES (?, ?, ?, ?, ?)',
            (driver_id, f'b155d{driver_id}', f'BUD155 Driver {driver_id}',
             number, f'B15{number}'))


def _insert_user(db, session_id):
    db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
               (session_id, _username(session_id)))


def _league_row(db, name):
    row = db.execute('SELECT * FROM leagues WHERE name = ?', (name,)).fetchone()
    assert row is not None, f'league {name!r} not found'
    return row


def _league_id(db, name):
    return _league_row(db, name)['id']


def _global_total(db, session_id):
    return db.execute(
        'SELECT COALESCE(SUM(points), 0) AS total FROM scores WHERE user_id = ?',
        (session_id,)).fetchone()['total']


def _rendered_total_for(html, username):
    """Pull the rendered season total for one username off a leaderboard
    page, anchored on the row's own <tr> (same shape as the F1-22 fixture's
    `_score_matrix_from_html` in test_league_standings_pure_filter.py), so a
    username that happens to be a substring elsewhere on the page (e.g.
    inside unrelated copy) can't be matched by accident."""
    m = re.search(
        r'<tr[^>]*>\s*<td>' + re.escape(username) + r'</td>\s*'
        r'(?:<td[^>]*>.*?</td>\s*)*'
        r'<td><span class="pts">(\d+)</span></td>', html)
    assert m, f'{username} row not found in rendered leaderboard'
    return int(m.group(1))


@pytest.fixture
def world(app, client):
    """1 completed race; 3 leagues all sharing MULTI as a member.

    MULTI submits exactly one prediction for the race; results are entered
    once via the existing admin path, producing exactly one `scores` row.
    """
    db = get_db()

    _insert_race(db)
    _insert_drivers(db)
    _insert_user(db, MULTI)
    _insert_user(db, NONMEMBER)
    db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
               ('bud155-admin', 'brett'))

    for name in LEAGUE_NAMES:
        db.execute(
            '''INSERT INTO leagues (name, emoji_or_color, start_round, whole_season, admin_user_id)
               VALUES (?, ?, 1, 0, ?)''',
            (name, '\U0001F3CE', MULTI))
        league_id = _league_id(db, name)
        db.execute(
            '''INSERT INTO league_members (league_id, user_id, joined_at_round, is_admin)
               VALUES (?, ?, 1, 1)''',
            (league_id, MULTI))

    # Non-member with points: sanity check it never leaks into a league view.
    db.execute(
        'INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) '
        'VALUES (?, ?, ?, ?, ?)', (NONMEMBER, RACE_ID) + PICK)
    db.execute('INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
               (NONMEMBER, RACE_ID, POINTS))
    db.commit()
    return db


def _submit_prediction_and_score_results(client, db):
    """MULTI submits their one prediction, then admin enters results.

    The race is fixture-seeded as already 'completed' (so the leaderboard
    sees a finished season), which means the live /predict route would
    reject a new submission - so the pick is inserted directly, exactly
    like the F1-22 fixture (test_league_standings_pure_filter.py) does.
    Scoring itself still goes through the real admin route.
    """
    db.execute(
        'INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) '
        'VALUES (?, ?, ?, ?, ?)', (MULTI, RACE_ID) + PICK)
    db.commit()

    _login(client, 'bud155-admin')
    resp = client.post(f'/admin/enter-results/{RACE_ID}',
                       data={'p1': PICK[0], 'p2': PICK[1], 'p3': PICK[2]},
                       follow_redirects=True)
    assert resp.status_code == 200
    assert b'Admin access only' not in resp.data, \
        'enter_results was not reachable as admin - fixture bug'


# ── AC1: N>=2 (here N=3) simultaneous membership rows ───────────────────────

class TestSimultaneousMembership:
    def test_user_holds_three_membership_rows_at_once(self, app, client, world):
        db = world
        rows = db.execute(
            'SELECT league_id FROM league_members WHERE user_id = ?', (MULTI,)
        ).fetchall()
        assert len(rows) == 3
        assert {r['league_id'] for r in rows} == {
            _league_id(db, name) for name in LEAGUE_NAMES}

    def test_get_user_leagues_lists_all_three(self, app, client, world):
        db = world
        leagues = get_user_leagues(db, MULTI)
        assert {l['name'] for l in leagues} == set(LEAGUE_NAMES)


# ── AC2: one prediction, one scores row, identical points everywhere ───────

class TestOnePredictionCountsEverywhere:
    def test_single_prediction_and_score_row_exist(self, app, client, world):
        db = world
        _submit_prediction_and_score_results(client, db)

        pred_count = db.execute(
            'SELECT COUNT(*) AS n FROM predictions WHERE user_id = ? AND race_id = ?',
            (MULTI, RACE_ID)).fetchone()['n']
        score_count = db.execute(
            'SELECT COUNT(*) AS n FROM scores WHERE user_id = ? AND race_id = ?',
            (MULTI, RACE_ID)).fetchone()['n']
        assert pred_count == 1, 'expected exactly one predictions row, no per-league duplicates'
        assert score_count == 1, 'expected exactly one scores row, no per-league duplicates'

    def test_all_three_leagues_and_global_show_identical_points(self, app, client, world):
        db = world
        _submit_prediction_and_score_results(client, db)

        expected = _global_total(db, MULTI)
        assert expected == POINTS

        # get_standings() read path (build_desk / home).
        for name in LEAGUE_NAMES:
            rows = {r['session_id']: r for r in get_standings(db, _league_row(db, name))}
            assert rows[MULTI]['podium_points'] == expected

        global_rows = {r['session_id']: r for r in get_standings(db)}
        assert global_rows[MULTI]['podium_points'] == expected

        # /leaderboard?league= read path.
        _login(client, MULTI)
        for name in LEAGUE_NAMES:
            lid = _league_id(db, name)
            html = client.get(f'/leaderboard?league={lid}').data.decode('utf-8')
            assert name in html
            assert _rendered_total_for(html, _username(MULTI)) == expected

        global_html = client.get('/leaderboard?season=current').data.decode('utf-8')
        assert _rendered_total_for(global_html, _username(MULTI)) == expected


# ── AC3: league switcher lists all leagues, no re-auth / no re-prediction ──

class TestLeagueSwitcher:
    def test_leagues_page_lists_all_three(self, app, client, world):
        db = world
        _login(client, MULTI)
        html = client.get('/leagues').data.decode('utf-8')
        for name in LEAGUE_NAMES:
            assert name in html

    def test_home_switcher_lists_all_three_leagues(self, app, client, world):
        db = world
        _login(client, MULTI)
        html = client.get('/home').data.decode('utf-8')
        for name in LEAGUE_NAMES:
            assert name in html

    def test_switching_leagues_reuses_session_and_prediction(self, app, client, world):
        """Navigating between leagues in the same session requires no new
        login and creates no new predictions row."""
        db = world
        _submit_prediction_and_score_results(client, db)
        _login(client, MULTI)

        for name in LEAGUE_NAMES:
            lid = _league_id(db, name)
            resp = client.get(f'/home?league={lid}')
            assert resp.status_code == 200
            # Still signed in as MULTI throughout - no redirect to login.
            assert b'Please sign in' not in resp.data

        pred_count = db.execute(
            'SELECT COUNT(*) AS n FROM predictions WHERE user_id = ? AND race_id = ?',
            (MULTI, RACE_ID)).fetchone()['n']
        assert pred_count == 1, 'switching leagues must not create new predictions'


# ── AC4: leaving one league leaves the others untouched ─────────────────────

class TestLeaveLeagueIsolated:
    def test_leaving_league_a_preserves_other_membership_and_standings(self, app, client, world):
        db = world
        _submit_prediction_and_score_results(client, db)

        league_a = _league_row(db, LEAGUE_NAMES[0])
        league_b = _league_row(db, LEAGUE_NAMES[1])
        league_c = _league_row(db, LEAGUE_NAMES[2])

        before_b = {r['session_id']: r['podium_points']
                    for r in get_standings(db, league_b)}
        before_global = _global_total(db, MULTI)

        remove_league_member(db, league_a['id'], MULTI)

        # Membership: gone from A, still present in B and C.
        assert not db.execute(
            'SELECT 1 FROM league_members WHERE league_id = ? AND user_id = ?',
            (league_a['id'], MULTI)).fetchone()
        for league in (league_b, league_c):
            assert db.execute(
                'SELECT 1 FROM league_members WHERE league_id = ? AND user_id = ?',
                (league['id'], MULTI)).fetchone()

        # Standings/points: B and global are byte-identical to before removal.
        after_b = {r['session_id']: r['podium_points']
                   for r in get_standings(db, league_b)}
        assert after_b == before_b
        assert _global_total(db, MULTI) == before_global

    def test_leave_route_removes_only_that_leagues_membership(self, app, client, world):
        db = world
        league_a = _league_row(db, LEAGUE_NAMES[0])
        _login(client, MULTI)

        resp = client.post(f'/leagues/{league_a["id"]}/leave', follow_redirects=True)
        assert resp.status_code == 200

        rows = db.execute(
            'SELECT league_id FROM league_members WHERE user_id = ?', (MULTI,)
        ).fetchall()
        assert len(rows) == 2
        assert league_a['id'] not in {r['league_id'] for r in rows}
