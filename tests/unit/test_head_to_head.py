"""BUD-157 / F1-27: head-to-head view — any two users' season accuracy compared.

Acceptance criteria covered (see Linear BUD-157):
  AC1  Given any two user identifiers, /h2h/<a>/<b> shows both users' season
       stats (total points, average points per race, count of races each
       out-scored the other) computed from the shared global `scores` table.
  AC2  Works for two users who have never shared a league (no common
       league_members row) — the route still returns a valid comparison.
  AC3  The page is linkable: a stable URL returns equivalent content on
       repeat GETs, with no required prior navigation state (a bare GET is
       enough — no need to visit /leaderboard first).
  AC4  Per-race points in the comparison match each user's individual
       per-race points exactly as shown on the global leaderboard's score
       matrix, for the same race — no separate scoring computation.
"""

import re

import pytest

from app import get_db

RACE_DATES = {
    1: '2026-03-08 14:00:00',
    2: '2026-03-22 14:00:00',
    3: '2026-04-05 14:00:00',
}

USER_A = 'bud157-a'
USER_B = 'bud157-b'
# Never in any league together, never even in the same league at all.
LONER_A = 'bud157-lonera'
LONER_B = 'bud157-lonerb'

DRIVERS = (1, 2, 3, 4, 5, 6)
PICK = (1, 2, 3)
POINTS_PER_ROUND = 20  # exact podium: 10 + 6 + 4


def _username(session_id):
    return session_id.split('-')[-1]


def _login(client, session_id):
    with client.session_transaction() as sess:
        sess['session_id'] = session_id


def _insert_race(db, race_id, round_num):
    db.execute(
        'INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
        (race_id, f'BUD157 GP {round_num}', round_num,
         RACE_DATES[round_num], 'completed'))


def _insert_drivers(db):
    for driver_id, number in enumerate(DRIVERS, start=1):
        db.execute(
            'INSERT INTO drivers (id, driver_id, name, number, code) '
            'VALUES (?, ?, ?, ?, ?)',
            (driver_id, f'b157d{driver_id}', f'BUD157 Driver {driver_id}',
             number, f'B15{number}'))


def _insert_user(db, session_id):
    db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
               (session_id, _username(session_id)))


def _insert_prediction(db, user_id, race_id, pick=PICK):
    db.execute(
        'INSERT INTO predictions (user_id, race_id, p1_driver_id, '
        'p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?, ?)',
        (user_id, race_id, pick[0], pick[1], pick[2]))


def _insert_score(db, user_id, race_id, points):
    db.execute('INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
               (user_id, race_id, points))


@pytest.fixture
def world(app, client):
    """3 completed races. USER_A scores all 3, USER_B scores 2 (misses R2).
    LONER_A / LONER_B each score races but share no league with anyone —
    used to prove the route needs no common league_members row.
    """
    db = get_db()

    for round_num in (1, 2, 3):
        _insert_race(db, round_num, round_num)
    _insert_drivers(db)

    for sid in (USER_A, USER_B, LONER_A, LONER_B):
        _insert_user(db, sid)

    for rid in (1, 2, 3):
        _insert_prediction(db, USER_A, rid)
        _insert_score(db, USER_A, rid, POINTS_PER_ROUND)

    # USER_B misses round 2 entirely (no prediction, no score row) and beats
    # USER_A on round 3 by a wider margin.
    _insert_prediction(db, USER_B, 1)
    _insert_score(db, USER_B, 1, 10)
    _insert_prediction(db, USER_B, 3)
    _insert_score(db, USER_B, 3, 26)

    for rid in (1, 2, 3):
        _insert_prediction(db, LONER_A, rid)
        _insert_score(db, LONER_A, rid, 4)
        _insert_prediction(db, LONER_B, rid)
        _insert_score(db, LONER_B, rid, 6)

    for rid in (1, 2, 3):
        db.execute(
            'INSERT INTO results (race_id, p1_driver_id, p2_driver_id, '
            'p3_driver_id) VALUES (?, 1, 2, 3)', (rid,))
    db.commit()
    return db


def _score_matrix_from_leaderboard(html):
    """Same extraction helper style used by the league-standings suite:
    username -> {round_index: rendered per-race cell}."""
    out = {}
    for m in re.finditer(
            r'<tr[^>]*>\s*<td>([^<]+)</td>\s*((?:<td[^>]*>.*?</td>\s*)+)'
            r'<td><span class="pts">.*?</span></td>', html):
        name = m.group(1).strip()
        cells = [c for c in re.findall(r'<td[^>]*>(.*?)</td>', m.group(2))]
        out[name] = cells
    return out


class TestHeadToHeadBasics:
    def test_route_returns_valid_comparison(self, app, client, world):
        _login(client, USER_A)
        resp = client.get(f'/h2h/{_username(USER_A)}/{_username(USER_B)}')
        assert resp.status_code == 200
        html = resp.data.decode('utf-8')
        assert _username(USER_A) in html
        assert _username(USER_B) in html

    def test_stats_computed_from_shared_scores_table(self, app, client, world):
        _login(client, USER_A)
        html = client.get(
            f'/h2h/{_username(USER_A)}/{_username(USER_B)}').data.decode('utf-8')

        # USER_A: 60 total / 3 races = 20.0 avg.
        assert '60' in html
        assert '20.0' in html
        # USER_B: 36 total / 2 races scored = 18.0 avg.
        assert '36' in html
        assert '18.0' in html

    def test_out_scored_counts(self, app, client, world):
        """R1: A(20) > B(10) -> A out-scores. R2: B has no score -> not
        counted either way. R3: B(26) > A(20) -> B out-scores. Net: 1-1."""
        _login(client, USER_A)
        html = client.get(
            f'/h2h/{_username(USER_A)}/{_username(USER_B)}').data.decode('utf-8')
        # Both counts are 1; just assert the page rendered the comparison
        # tile with both users' out-scored counts distinctly from a 0/0 wash.
        assert html.count('>1<') >= 2


class TestNoSharedLeagueRequired:
    def test_route_works_for_pair_with_no_common_league_row(self, app, client, world):
        db = world
        # Prove there truly is no league_members row linking these two.
        common = db.execute('''
            SELECT COUNT(*) AS n FROM league_members lm1
            JOIN league_members lm2 ON lm1.league_id = lm2.league_id
            WHERE lm1.user_id = ? AND lm2.user_id = ?
        ''', (LONER_A, LONER_B)).fetchone()['n']
        assert common == 0, 'fixture bug: loners share a league row'

        _login(client, LONER_A)
        resp = client.get(f'/h2h/{_username(LONER_A)}/{_username(LONER_B)}')
        assert resp.status_code == 200
        html = resp.data.decode('utf-8')
        assert _username(LONER_A) in html
        assert _username(LONER_B) in html
        # 3 races x 4/6 points each.
        assert '12' in html  # LONER_A total
        assert '18' in html  # LONER_B total


class TestLinkableStableUrl:
    def test_repeat_get_is_equivalent_with_no_prior_navigation(self, app, client, world):
        """A fresh client, no visit to /leaderboard or anywhere else first —
        the URL alone must be enough."""
        _login(client, USER_A)
        first = client.get(f'/h2h/{_username(USER_A)}/{_username(USER_B)}').data
        second = client.get(f'/h2h/{_username(USER_A)}/{_username(USER_B)}').data
        assert first == second

    def test_order_of_identifiers_both_resolve(self, app, client, world):
        """/h2h/a/b and /h2h/b/a both work (same two users, either order)."""
        _login(client, USER_A)
        r1 = client.get(f'/h2h/{_username(USER_A)}/{_username(USER_B)}')
        r2 = client.get(f'/h2h/{_username(USER_B)}/{_username(USER_A)}')
        assert r1.status_code == 200
        assert r2.status_code == 200

    def test_unknown_identifier_is_404_not_500(self, app, client, world):
        _login(client, USER_A)
        resp = client.get(f'/h2h/{_username(USER_A)}/does-not-exist')
        assert resp.status_code == 404


class TestPerRacePointsMatchLeaderboard:
    def test_per_race_points_identical_to_leaderboard_score_matrix(self, app, client, world):
        """AC4: same per-race numbers as /leaderboard's score_matrix, for
        both users, for every race either of them has a score in."""
        db = world
        _login(client, USER_A)

        global_html = client.get('/leaderboard?season=current').data.decode('utf-8')
        global_matrix = _score_matrix_from_leaderboard(global_html)

        h2h_html = client.get(
            f'/h2h/{_username(USER_A)}/{_username(USER_B)}').data.decode('utf-8')
        h2h_matrix = _score_matrix_from_leaderboard(h2h_html)

        assert global_matrix[_username(USER_A)] == h2h_matrix[_username(USER_A)]
        assert global_matrix[_username(USER_B)] == h2h_matrix[_username(USER_B)]

    def test_h2h_route_issues_no_separate_scoring_aggregation_query(self, app, client, world, monkeypatch):
        """Static inspection: the h2h per-race lookup is the same single-row
        `SELECT points FROM scores WHERE user_id = ? AND race_id = ?` used by
        the leaderboard's score_matrix — not a fresh SUM/GROUP BY query."""
        from pathlib import Path
        src = (Path(__file__).resolve().parents[2] / 'src' / 'app.py').read_text()
        h2h_start = src.index('def _h2h_user_matrix')
        h2h_end = src.index('def head_to_head(')
        section = src[h2h_start:h2h_end]
        assert 'SELECT points FROM scores WHERE user_id = ? AND race_id = ?' in section
        assert 'SUM(' not in section
        assert 'GROUP BY' not in section.upper()


class TestPickerEntryPoint:
    def test_picker_redirects_to_canonical_url(self, app, client, world):
        _login(client, USER_A)
        resp = client.get(f'/h2h?a={_username(USER_A)}&b={_username(USER_B)}')
        assert resp.status_code == 302
        assert resp.headers['Location'].endswith(
            f'/h2h/{_username(USER_A)}/{_username(USER_B)}')

    def test_picker_without_params_renders_form(self, app, client, world):
        _login(client, USER_A)
        resp = client.get('/h2h')
        assert resp.status_code == 200
        assert b'Compare' in resp.data

    def test_leaderboard_links_to_picker(self, app, client, world):
        _login(client, USER_A)
        html = client.get('/leaderboard?season=current').data.decode('utf-8')
        assert '/h2h' in html
