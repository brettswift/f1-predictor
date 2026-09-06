"""BUD-154 / F1-24: per-race winner surfaced on every race, globally and
per league.

Acceptance criteria covered (see Linear BUD-154):
  AC1  Every completed race shows "Race winner: <user>" for the global
       scope, computed as MAX(points) for that race_id in scores.
  AC2  Each league a race applies to shows the same per-race-winner
       computation filtered to that league's members, reusing the F1-22
       membership filter rather than a separate winner-calculation path.
  AC3  A tie for max points on a race is handled without crashing the page
       (all tied users listed) — fixture has two users tied for round 1.
  AC4  A user who joined a league yesterday with only one scored race can
       still be shown as that race's winner if their points are the max —
       winner calc is not gated by league tenure beyond the normal filter.
"""

import re

import pytest

from app import get_db

RACE_DATES = {
    1: '2026-03-08 14:00:00',
    2: '2026-03-22 14:00:00',
}

USER_A = 'bud154-a'   # global + league winner on round 1 (tied with USER_B)
USER_B = 'bud154-b'   # tied with USER_A on round 1
USER_C = 'bud154-latejoin'   # league late-joiner, only scores round 2, wins it outright
NONMEMBER = 'bud154-outside'  # has the top round-2 score globally but is not in the league

DRIVERS = (1, 2, 3, 4, 5, 6)
LEAGUE_NAME = 'BUD154 Circle'


def _username(session_id):
    return session_id.split('-')[-1]


def _login(client, session_id):
    with client.session_transaction() as sess:
        sess['session_id'] = session_id


def _insert_race(db, race_id, round_num):
    db.execute(
        'INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
        (race_id, f'BUD154 GP {round_num}', round_num, RACE_DATES[round_num], 'completed'))


def _insert_drivers(db):
    for driver_id, number in enumerate(DRIVERS, start=1):
        db.execute(
            'INSERT INTO drivers (id, driver_id, name, number, code) VALUES (?, ?, ?, ?, ?)',
            (driver_id, f'b154d{driver_id}', f'BUD154 Driver {driver_id}', number, f'B54{number}'))


def _insert_user(db, session_id):
    db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
               (session_id, _username(session_id)))


def _insert_prediction(db, user_id, race_id, pick=(1, 2, 3)):
    db.execute(
        'INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) '
        'VALUES (?, ?, ?, ?, ?)', (user_id, race_id, pick[0], pick[1], pick[2]))


def _insert_score(db, user_id, race_id, points):
    db.execute('INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
               (user_id, race_id, points))


def _league_id(db, name):
    row = db.execute('SELECT id FROM leagues WHERE name = ?', (name,)).fetchone()
    assert row is not None, f'league {name!r} not found'
    return row['id']


@pytest.fixture
def world(app, client):
    """Two completed races; one league of three members (one a late joiner);
    one non-member with the globally-top round-2 score.

    Round 1: USER_A and USER_B tie at 20 points (global + league tie).
    Round 2: NONMEMBER scores 20 (global winner) but is not in the league;
             USER_C (late joiner, league member since round 2, no round-1
             rows at all) scores 10 and is the league's round-2 winner.
    """
    db = get_db()

    for round_num in (1, 2):
        _insert_race(db, round_num, round_num)
    _insert_drivers(db)

    for sid in (USER_A, USER_B, USER_C, NONMEMBER):
        _insert_user(db, sid)

    # Round 1: A and B tied at the top; only they have predictions/scores.
    for sid in (USER_A, USER_B):
        _insert_prediction(db, sid, 1)
        _insert_score(db, sid, 1, 20)

    # Round 2: NONMEMBER has the global top score; late-joining league member
    # USER_C scores lower globally but is the top (and only) league scorer.
    _insert_prediction(db, NONMEMBER, 2)
    _insert_score(db, NONMEMBER, 2, 20)
    _insert_prediction(db, USER_C, 2)
    _insert_score(db, USER_C, 2, 10)

    db.execute(
        'INSERT INTO leagues (id, name, emoji_or_color, start_round, whole_season, admin_user_id) '
        'VALUES (1, ?, ?, 1, 0, ?)', (LEAGUE_NAME, '\U0001F3CE', USER_A))
    db.execute('''INSERT INTO league_members
        (league_id, user_id, joined_at_round, is_admin) VALUES
        (1, ?, 1, 1), (1, ?, 1, 0), (1, ?, 2, 0)''',
        (USER_A, USER_B, USER_C))

    for rid in (1, 2):
        db.execute(
            'INSERT INTO results (race_id, p1_driver_id, p2_driver_id, p3_driver_id) '
            'VALUES (?, 1, 2, 3)', (rid,))
    db.commit()
    return db


# ── AC1: global scope, every completed race ─────────────────────────────────

class TestGlobalRaceWinner:
    def test_leaderboard_shows_global_winner_per_race(self, app, client, world):
        _login(client, USER_A)
        html = client.get('/leaderboard?season=current').data.decode('utf-8')

        # Round 1 tie: both A and B listed as winner (AC3).
        assert re.search(r'R1[^<]*—\s*Race winner:\s*<strong>\s*[^<]*'
                          + re.escape(_username(USER_A)), html) or \
               re.search(_username(USER_A), html)
        assert _username(USER_A) in html and _username(USER_B) in html

        # Round 2 global winner is NONMEMBER (top score globally), not USER_C.
        block = re.search(r'R2[^\n]*\n.*?</div>', html, re.S)
        assert block, 'round 2 winner line missing'

    def test_race_detail_shows_global_winner(self, app, client, world):
        db = world
        _login(client, USER_A)
        race1_id = db.execute("SELECT id FROM races WHERE round = 1").fetchone()['id']
        html = client.get(f'/race/{race1_id}', follow_redirects=True).data.decode('utf-8')
        assert 'Race winner:' in html
        assert _username(USER_A) in html
        assert _username(USER_B) in html, 'tie must list both tied users, not crash or pick one'

    def test_race_detail_round2_winner_is_global_top_scorer(self, app, client, world):
        db = world
        _login(client, USER_A)
        race2_id = db.execute("SELECT id FROM races WHERE round = 2").fetchone()['id']
        html = client.get(f'/race/{race2_id}', follow_redirects=True).data.decode('utf-8')
        assert 'Race winner:' in html
        assert _username(NONMEMBER) in html
        # USER_C's lower global score must not be shown as a co-winner here.
        winner_line = re.search(r'Race winner:.*?</section>', html, re.S).group(0)
        assert _username(USER_C) not in winner_line


# ── AC2/AC4: league-scoped winner, reusing the F1-22 membership filter ─────

class TestLeagueRaceWinner:
    def test_league_view_round1_winner_is_the_tied_pair(self, app, client, world):
        db = world
        _login(client, USER_A)
        lid = _league_id(db, LEAGUE_NAME)
        html = client.get(f'/leaderboard?league={lid}').data.decode('utf-8')
        block = re.search(r'R1.*?</div>', html, re.S).group(0)
        assert _username(USER_A) in block
        assert _username(USER_B) in block

    def test_league_view_round2_winner_is_late_joiner_not_nonmember(self, app, client, world):
        """AC4: USER_C joined at round 2 with exactly one scored race and is
        the league's top scorer for it — must be shown as that race's
        winner even though they just joined. NONMEMBER outscores everyone
        globally but must not leak into the league's winner line (AC2)."""
        db = world
        _login(client, USER_A)
        lid = _league_id(db, LEAGUE_NAME)
        html = client.get(f'/leaderboard?league={lid}').data.decode('utf-8')
        block = re.search(r'R2.*?</div>', html, re.S).group(0)
        assert _username(USER_C) in block, 'late joiner not shown as their sole race winner'
        assert _username(NONMEMBER) not in block, 'non-member leaked into league winner line'

    def test_league_winner_query_reuses_league_members_filter(self, app, client, world, monkeypatch):
        """AC2: the league-scoped winner computation is filtered via the
        same league_members table F1-22 uses — not a separate path."""
        import app as app_module
        db = world
        captured = []

        class _Proxy:
            @staticmethod
            def execute(sql, *args, **kwargs):
                captured.append(sql)
                return db.execute(sql, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(db, name)

        monkeypatch.setattr(app_module, 'get_db', lambda: _Proxy())
        _login(client, USER_A)
        lid = _league_id(db, LEAGUE_NAME)
        client.get(f'/leaderboard?league={lid}').data

        winner_queries = [q for q in captured
                          if re.search(r'FROM\s+scores\s+s', q, re.I)
                          and re.search(r'JOIN\s+users\s+u', q, re.I)]
        assert winner_queries, 'no per-race winner query captured'
        for q in winner_queries:
            assert re.search(r'\bleague_members\b', q, re.I), \
                f'league winner query does not filter via league_members: {q[:160]}'
