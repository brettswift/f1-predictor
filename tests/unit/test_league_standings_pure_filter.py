"""BUD-152 / F1-22: league standings are a pure filter over global scores.

Epic E3 ("one game, many views") — the load-bearing invariant of the epic:
a league's standings are produced by filtering the *same* scores table the
global leaderboard reads. There is no scoring window, no `start_round`
gating, no per-league scores table or cache.

Acceptance criteria covered (see Linear BUD-152):
  AC1  League standings are a pure filter of the global scores table, with
       no separate scoring computation, write path, or cache.
  AC2  With >=2 leagues sharing >=1 common member and >=2 completed races,
       that member's per-race points rendered by every league view they
       belong to are byte-identical to the global leaderboard's per-race
       points, for every race in the comparison — asserted against BOTH
       read paths: /leaderboard?league= and get_standings() (used by the
       /home desk via build_desk).
  AC3  A league's total for any member equals that member's global total
       for the season — one assertion covers every member, early-joiner
       or late.
  AC4  A late joiner (round 5) has zero predictions/scores rows for rounds
       1-4 and their league total naturally reflects only rounds 5+ — no
       code branching on join date.
  AC5  Recomputing a score once via the existing enter_results admin path
       updates the value identically in the global view and in every
       league view on the next read.
  AC6  No scoring write path keys scores by league_id (static source
       inspection + live check that the admin correction only rewrites
       (user_id, race_id, points) rows).
"""

import re

import pytest

from app import get_db, get_standings

# ── Fixture constants ───────────────────────────────────────────────────────
# One season of races; dates are all past so they compute as 'completed'
# once results exist, and the league view sees the same race set as the
# global leaderboard.
RACE_DATES = {
    1: '2026-03-08 14:00:00',
    2: '2026-03-22 14:00:00',
    3: '2026-04-05 14:00:00',
    4: '2026-04-19 14:00:00',
    5: '2026-05-03 14:00:00',
}

USER_A = 'bud152-a'   # early joiner, in both leagues, scores all 5 rounds
USER_B = 'bud152-b'   # early joiner, in both leagues, scores all 5 rounds
USER_C = 'bud152-c'   # late joiner (round 5), in both leagues, scores round 5 only
NONMEMBER = 'bud152-outside'  # has points, belongs to no league

DRIVERS = (1, 2, 3, 4, 5, 6)

LEAGUE_A = 'BUD152 Alpha'
LEAGUE_B = 'BUD152 Bravo'

# Both leagues are "windowed" on paper (A: round 1 forward, B: round 3
# forward) — the exact state the pre-2026-09-03 code would have windowed.
# Scoring must ignore the window entirely.
LEAGUE_WINDOWS = {LEAGUE_A: (1, 1, 0), LEAGUE_B: (2, 3, 0)}  # (id, start_round, whole_season)

# Identical podium pick for every scored round: A wins, B second, C third.
PICK = (1, 2, 3)
# Round-1 correction: podium drawn from drivers 4-6, disjoint from the
# fixture's picks, so every fixture user's round-1 score drops to 0.
CORRECTION = (4, 5, 6)

POINTS_PER_ROUND = 20  # exact podium: 10 + 6 + 4
SCORED_ROUNDS = (1, 2, 3, 4, 5)


# ── Helpers ─────────────────────────────────────────────────────────────────

def _username(session_id):
    return session_id.split('-')[-1]


def _login(client, session_id):
    with client.session_transaction() as sess:
        sess['session_id'] = session_id


def _insert_race(db, race_id, round_num):
    db.execute(
        'INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
        (race_id, f'BUD152 GP {round_num}', round_num,
         RACE_DATES[round_num], 'completed'))


def _insert_drivers(db):
    for driver_id, number in enumerate(DRIVERS, start=1):
        db.execute(
            'INSERT INTO drivers (id, driver_id, name, number, code) '
            'VALUES (?, ?, ?, ?, ?)',
            (driver_id, f'b152d{driver_id}', f'BUD152 Driver {driver_id}',
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


def _insert_admin(db):
    # 'brett' is in ADMIN_USERNAMES — the enter_results route requires it.
    db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
               ('bud152-admin', 'brett'))
    db.commit()


def _enter_results(client, race_id, p1, p2, p3):
    """Admin POST to the existing enter_results path (src/app.py)."""
    _login(client, 'bud152-admin')
    resp = client.post(f'/admin/enter-results/{race_id}',
                       data={'p1': p1, 'p2': p2, 'p3': p3},
                       follow_redirects=True)
    assert resp.status_code == 200
    assert b'Admin access only' not in resp.data, \
        'enter_results was not reachable as admin — fixture bug'


def _score_matrix_from_html(html):
    """Extract the rendered race-by-race matrix: username -> {round: cell}.

    Parses the actual rendered cells, so the comparison is byte-identical
    to what the user sees, not a re-query of the DB.
    """
    out = {}
    for m in re.finditer(
            r'<tr[^>]*>\s*<td>([^<]+)</td>\s*((?:<td[^>]*>.*?</td>\s*)+)'
            r'<td><span class="pts">.*?</span></td>', html):
        name = m.group(1).strip()
        cells = [c for c in re.findall(r'<td[^>]*>(.*?)</td>', m.group(2))]
        out[name] = {rid: cell.strip() for rid, cell in zip(SCORED_ROUNDS, cells)}
    return out


def _matrix_total(cells):
    return sum(int(v) for v in cells.values() if v.isdigit())


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


def _capture_queries(monkeypatch, real_db):
    """Route every DB query of the next requests through a capturing proxy."""
    import app as app_module
    captured = []

    class _Proxy:
        @staticmethod
        def execute(sql, *args, **kwargs):
            captured.append(sql)
            return real_db.execute(sql, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(real_db, name)

    monkeypatch.setattr(app_module, 'get_db', lambda: _Proxy())
    return captured


# ── Fixture ─────────────────────────────────────────────────────────────────

@pytest.fixture
def world(app, client):
    """5 completed races; 2 leagues sharing 3 members; 1 non-member.

    user_c joins at round 5: they have NO predictions or scores rows for
    rounds 1-4, so their league total naturally reflects only round 5+.
    """
    db = get_db()

    for round_num in SCORED_ROUNDS:
        _insert_race(db, round_num, round_num)
    _insert_drivers(db)

    for sid in (USER_A, USER_B, USER_C, NONMEMBER):
        _insert_user(db, sid)

    # Early joiners score all five rounds; the late joiner scores only 5.
    for sid in (USER_A, USER_B):
        for rid in SCORED_ROUNDS:
            _insert_prediction(db, sid, rid)
            _insert_score(db, sid, rid, POINTS_PER_ROUND)
    _insert_prediction(db, USER_C, 5)
    _insert_score(db, USER_C, 5, POINTS_PER_ROUND)

    # Non-member with points: must never appear in a league view.
    _insert_prediction(db, NONMEMBER, 1)
    _insert_score(db, NONMEMBER, 1, POINTS_PER_ROUND)

    # Two leagues with the same membership, each carrying a paper window
    # (round 1 forward / round 3 forward) that scoring must ignore.
    for name, (lid, start_round, whole_season) in LEAGUE_WINDOWS.items():
        db.execute(
            'INSERT INTO leagues (id, name, emoji_or_color, start_round, '
            'whole_season, admin_user_id) VALUES (?, ?, ?, ?, ?, ?)',
            (lid, name, '\U0001F3CE' if name == LEAGUE_A else '\U0001F1FA',
             start_round, whole_season, USER_A if name == LEAGUE_A else USER_B))
    db.execute('''INSERT INTO league_members
        (league_id, user_id, joined_at_round, is_admin) VALUES
        (1, ?, 1, 1), (1, ?, 1, 0), (1, ?, 5, 0),
        (2, ?, 1, 0), (2, ?, 1, 1), (2, ?, 5, 0)''',
        (USER_A, USER_B, USER_C, USER_A, USER_B, USER_C))

    # Complete all five rounds with the initial result.
    for rid in SCORED_ROUNDS:
        db.execute(
            'INSERT INTO results (race_id, p1_driver_id, p2_driver_id, '
            'p3_driver_id) VALUES (?, 1, 2, 3)', (rid,))
    _insert_admin(db)
    db.commit()
    return db


# ── AC2: per-race byte-identity across both read paths ──────────────────────

class TestPerRacePointsIdentical:
    """>=2 leagues sharing members + >=2 completed races: every league view
    shows the common members' per-race points byte-identically to the global
    leaderboard — for /leaderboard AND for get_standings() (the desk)."""

    def test_leaderboard_matrix_byte_identical_in_both_leagues(self, app, client, world):
        db = world
        _login(client, USER_A)

        global_html = client.get('/leaderboard?season=current').data.decode('utf-8')
        global_matrix = _score_matrix_from_html(global_html)
        # Sanity: the global view really rendered all five scored rounds.
        assert set(global_matrix[_username(USER_A)]) == set(SCORED_ROUNDS)

        for league in (LEAGUE_A, LEAGUE_B):
            lid = _league_id(db, league)
            html = client.get(f'/leaderboard?league={lid}').data.decode('utf-8')
            assert league in html, f'{league} tag missing from league view'
            matrix = _score_matrix_from_html(html)

            for sid in (USER_A, USER_B, USER_C):
                assert matrix[_username(sid)] == global_matrix[_username(sid)], \
                    (f'{league}: {sid} per-race cells differ from global: '
                     f'{matrix[_username(sid)]} != {global_matrix[_username(sid)]}')
            # The non-member must not leak into the league view.
            assert _username(NONMEMBER) not in matrix

    def test_get_standings_podium_identical_to_global(self, app, client, world):
        """Second read path (build_desk → get_standings, used by / and /home):
        each member's podium total must equal their global season total."""
        db = world
        _login(client, USER_A)

        for league in (LEAGUE_A, LEAGUE_B):
            rows = {r['session_id']: r for r in get_standings(db, _league_row(db, league))}
            assert set(rows) == {USER_A, USER_B, USER_C}, \
                f'{league}: league field is not exactly its members'
            for sid in (USER_A, USER_B, USER_C):
                assert rows[sid]['podium_points'] == _global_total(db, sid), \
                    (f'{league}: {sid} podium {rows[sid]["podium_points"]} != '
                     f'global total {_global_total(db, sid)}')

    def test_home_desk_renders_unwindowed_league_totals(self, app, client, world):
        """The /home desk renders get_standings() output; a league whose
        paper window starts at round 3 must NOT hide rounds 1-2."""
        db = world
        _login(client, USER_B)

        lid = _league_id(db, LEAGUE_B)
        html = client.get(f'/home?league={lid}').data.decode('utf-8')

        # Each early joiner's full-season podium total (5 x 20 = 100) is
        # shown on the desk; under the old start_round windowing it would
        # show 3 x 20 = 60 for a round-3 window.
        for sid in (USER_A, USER_B):
            m = re.search(
                r'<tr[^>]*>\s*<td><span class="pos-n">\d+</span></td>\s*<td>'
                r'<span class="who">[\s\S]*?<span>' + re.escape(_username(sid))
                + r'</span>[\s\S]*?</span></td>\s*<td[^>]*>'
                r'<span class="num" style="font-size:12px;color:var\(--dim\)">'
                r'(\d+)</span>', html)
            assert m, f'{sid} row missing from the desk standings'
            assert int(m.group(1)) == _global_total(db, sid), \
                (f'desk podium for {sid} is {m.group(1)}; expected the full '
                 f'season total {_global_total(db, sid)} (windowing applied?)')


# ── AC3: league total == global total, every member ────────────────────────

class TestLeagueTotalEqualsGlobalTotal:
    def test_every_league_every_member_total_matches_global(self, app, client, world):
        db = world
        for league in (LEAGUE_A, LEAGUE_B):
            rows = {r['session_id']: r for r in get_standings(db, _league_row(db, league))}
            for sid in (USER_A, USER_B, USER_C):
                assert rows[sid]['podium_points'] == _global_total(db, sid)

    def test_leaderboard_totals_match_global_per_member(self, app, client, world):
        db = world
        _login(client, USER_A)
        global_html = client.get('/leaderboard?season=current').data.decode('utf-8')
        global_matrix = _score_matrix_from_html(global_html)

        for league in (LEAGUE_A, LEAGUE_B):
            lid = _league_id(db, league)
            html = client.get(f'/leaderboard?league={lid}').data.decode('utf-8')
            lmatrix = _score_matrix_from_html(html)
            for sid in (USER_A, USER_B, USER_C):
                global_total = _matrix_total(global_matrix[_username(sid)])
                league_total = _matrix_total(lmatrix[_username(sid)])
                assert league_total == global_total, \
                    (f'{league}: {sid} league total {league_total} != '
                     f'global {global_total}')
                assert league_total == _global_total(db, sid)


# ── AC4: late joiner — no code branching on join date ──────────────────────

class TestLateJoiner:
    def test_late_joiner_has_no_rows_before_join_round(self, app, client, world):
        db = world
        for table in ('predictions', 'scores'):
            for rid in (1, 2, 3, 4):
                n = db.execute(
                    f'SELECT COUNT(*) AS n FROM {table} '
                    'WHERE user_id = ? AND race_id = ?',
                    (USER_C, rid)).fetchone()['n']
                assert n == 0, \
                    f'late joiner has {table} rows for pre-join race {rid}'

        # Their total in the league is naturally just round 5.
        rows = {r['session_id']: r
                for r in get_standings(db, _league_row(db, LEAGUE_A))}
        assert rows[USER_C]['podium_points'] == POINTS_PER_ROUND
        assert rows[USER_C]['podium_points'] == _global_total(db, USER_C)

    def test_late_joiner_ranked_beneath_early_joiners_naturally(self, app, client, world):
        db = world
        rows = get_standings(db, _league_row(db, LEAGUE_A))
        positions = {r['session_id']: r['position'] for r in rows}
        assert positions[USER_C] > positions[USER_A]
        assert positions[USER_C] > positions[USER_B]


class TestLeagueViewAccess:
    def test_non_member_falls_back_to_global_view(self, app, client, world):
        """?league= for a league you don't belong to must not leak its table —
        the view falls back to the global standings (mirrors how the /home
        desk silently ignores a league you are not in)."""
        db = world
        _login(client, NONMEMBER)
        html = client.get(f'/leaderboard?league={_league_id(db, LEAGUE_A)}').data.decode('utf-8')
        assert LEAGUE_A not in html, 'non-member rendered a league view'
        matrix = _score_matrix_from_html(html)
        assert { _username(USER_A), _username(USER_B), _username(NONMEMBER) } <= set(matrix)


# ── AC5: recomputation updates every view identically ───────────────────────

class TestRecomputePropagatesToAllViews:
    def test_admin_correction_updates_global_and_league_identically(self, app, client, world):
        db = world
        assert _global_total(db, USER_A) == POINTS_PER_ROUND * 5

        # Admin corrects round 1: podium reversed -> USER_A's 20 pts drop to 0.
        _enter_results(client, 1, *CORRECTION)

        expected = _global_total(db, USER_A)
        assert expected == POINTS_PER_ROUND * 4, \
            f'correction did not recompute the score row: got {expected}'

        # Global view on next read…
        _login(client, USER_A)
        global_html = client.get('/leaderboard?season=current').data.decode('utf-8')
        gmatrix = _score_matrix_from_html(global_html)
        assert gmatrix[_username(USER_A)][1] == '0', \
            'global leaderboard still shows the pre-correction round-1 score'

        # …and every league view the member belongs to, per-race and total.
        for league in (LEAGUE_A, LEAGUE_B):
            lid = _league_id(db, league)
            html = client.get(f'/leaderboard?league={lid}').data.decode('utf-8')
            matrix = _score_matrix_from_html(html)
            assert matrix[_username(USER_A)][1] == '0', \
                f'{league}: stale round-1 score {matrix[_username(USER_A)][1]!r} after correction'
            for rid in SCORED_ROUNDS:
                assert matrix[_username(USER_A)][rid] == gmatrix[_username(USER_A)][rid]
            rows = {r['session_id']: r
                    for r in get_standings(db, _league_row(db, league))}
            assert rows[USER_A]['podium_points'] == expected, \
                f'{league}: desk total {rows[USER_A]["podium_points"]} != global {expected}'

    def test_correction_touches_no_league_keyed_state(self, app, client, world):
        """AC6 (live half): the correction rewrites (user_id, race_id, points)
        rows only — exactly one scores row per user per race, nothing else."""
        db = world
        _enter_results(client, 1, *CORRECTION)

        dups = db.execute(
            'SELECT user_id, race_id, COUNT(*) AS n FROM scores '
            'GROUP BY user_id, race_id HAVING n > 1').fetchall()
        assert dups == [], 'correction duplicated scores rows'

        n = db.execute(
            'SELECT COUNT(*) AS n FROM scores WHERE user_id = ? AND race_id = 1',
            (USER_A,)).fetchone()['n']
        assert n == 1
        assert db.execute(
            'SELECT points FROM scores WHERE user_id = ? AND race_id = 1',
            (USER_A,)).fetchone()['points'] == 0


# ── AC1/AC6: no league-keyed writes, pure-filter queries ────────────────────

class TestNoLeagueScoringWritePath:
    def test_scores_writes_are_league_agnostic_in_source(self):
        """Static inspection: every INSERT into scores uses only
        (user_id, race_id, points); none is keyed by league_id."""
        from pathlib import Path
        src = (Path(__file__).resolve().parents[2] / 'src' / 'app.py').read_text()
        insert_idx = [m.start() for m in re.finditer(r'INSERT\s+INTO\s+scores', src, re.I)]
        assert insert_idx, 'expected scoring write paths to still exist in src/app.py'
        for idx in insert_idx:
            window = src[idx:idx + 220]
            assert 'league_id' not in window.lower(), \
                f'scoring write is keyed by league_id: {window[:120]!r}'
            assert re.search(r'ON\s+CONFLICT\s*\(\s*user_id\s*,\s*race_id\s*\)',
                             window, re.I), \
                f'scoring write lost its (user_id, race_id) uniqueness: {window[:120]!r}'

    def test_global_leaderboard_query_has_no_league_reference(self, app, client, world, monkeypatch):
        """The global read path must not reference league tables, even with
        leagues + memberships present (mirrors the BUD-149 anchor)."""
        db = world
        _login(client, USER_A)
        captured = _capture_queries(monkeypatch, db)

        html = client.get('/leaderboard?season=current').data.decode('utf-8')
        assert _username(USER_A) in html

        for q in captured:
            assert not re.search(r'\bleague_members\b', q, re.I), \
                f'global standings path references league_members: {q[:160]}'

    def test_league_leaderboard_query_is_a_pure_filter(self, app, client, world, monkeypatch):
        """The league read path filters by membership only — no round/window
        predicate, no start_round in the query (the 2026-09-03 rule)."""
        db = world
        _login(client, USER_A)
        captured = _capture_queries(monkeypatch, db)

        client.get('/leaderboard?league=1').data

        standings = [q for q in captured
                     if re.search(r'SUM\s*\(\s*(s\.)?points\s*\)', q, re.I)
                     and re.search(r'GROUP\s+BY', q, re.I)]
        assert standings, 'no standings aggregation query captured for the league view'
        for q in standings:
            assert re.search(r'\bleague_members\b', q, re.I), \
                f'league view does not filter via league_members: {q[:160]}'
            assert not re.search(r'r\.round\s*>=', q, re.I), \
                f'league standings still apply a round window: {q[:160]}'
            assert 'start_round' not in q.lower()
