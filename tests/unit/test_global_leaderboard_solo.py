"""BUD-149 / F1-19: Global leaderboard as a first-class destination.

Epic E3 anchor / backstop: the global leaderboard is computed by summing the
single `scores` table, and a solo user (zero league memberships) is a
first-class citizen of it. Later E3 stories (F1-20/F1-21/F1-25 league tables,
F1-22 league standings, F1-23 ranking modes) must not regress either
property. These tests are the invariant the rest of the epic layers on top
of — keep them running in CI as those stories merge.

Acceptance criteria covered:
  AC1  Solo user (no league memberships) reaches /leaderboard via primary
       navigation, gets HTTP 200, and sees their own rank row.
  AC2  No page reachable by a solo user (leaderboard / home / live) contains
       copy gating them on league membership (denylist scan of rendered HTML).
  AC3  A solo user's global total_score on the leaderboard is unchanged
       (same integer) with league tables + league memberships present in the
       schema vs. absent — the "after" state later E3 stories create.
  AC4  Global standings are still computed by summing the single `scores`
       table: the executed query is SUM(points) ... GROUP BY user, with no
       join to any league table (verified at runtime, not just by eye).
"""

import re

import app as app_module

# Phrases that would gate a solo user on league membership. Scanned
# case-insensitively against the rendered HTML of every page a solo user can
# reach. Benign league copy ("No leagues yet", "Join a league" inside the
# invite flow) is intentionally NOT on this list — only gate/CTA phrasing.
GATING_DENYLIST = (
    'join a league to see your rank',
    'join a league to continue',
    'join a league to view',
    'join a league to see',
    'join a league to get started',
    'join a league first',
    'join a league to unlock',
    'must join a league',
    'need to join a league',
    'join any league to',
)

SOLO_SID = 'solo-bud149'


def _render(client, path):
    """GET a page as the solo user; fail hard if it is not a real 200 page."""
    response = client.get(path)
    assert response.status_code == 200, f'{path}: expected 200, got {response.status_code}'
    return response.data.decode('utf-8')


def _standings_row(html):
    """Return the main standings-table row (class="me") of the solo user."""
    m = re.search(r'<tr[^>]*class="me"[^>]*>[\s\S]*?</tr>', html)
    assert m, 'solo user has no highlighted rank row on the leaderboard'
    return m.group(0)


def _podium_score(row_html):
    """The solo user's global total_score as rendered in the standings table.

    This is the raw SUM(points) column ("Podium"), not the "Total" column,
    which additionally includes the safety-car pool value.
    """
    m = re.search(r'style="font-size:12px;color:var\(--dim\)">(-?\d+)</span>', row_html)
    assert m, f'could not extract the solo user\'s total_score from the row: {row_html!r}'
    return int(m.group(1))


def _create_solo_user(db):
    """A signed-in user with scores and zero league memberships."""
    db.execute(
        'INSERT INTO users (session_id, username) VALUES (?, ?)',
        (SOLO_SID, 'solosolo'))
    # Two scored rounds in the current season so the rank row is meaningful.
    for race_id, date, points in ((4001, '2026-03-15 14:00:00', 20),
                                  (4002, '2026-04-05 14:00:00', 10)):
        db.execute(
            'INSERT INTO races (id, name, round, date, status) '
            'VALUES (?, ?, ?, ?, ?)',
            (race_id, f'BUD149 GP {race_id}', race_id, date, 'completed'))
        db.execute(
            'INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
            (SOLO_SID, race_id, points))
    db.commit()


def _log_in(client, session_id=SOLO_SID):
    with client.session_transaction() as sess:
        sess['session_id'] = session_id


class TestSoloUserLeaderboard:
    """AC1: a solo user reaches the global leaderboard and sees their rank."""

    def test_solo_user_reaches_leaderboard_and_sees_own_rank(self, app, client):
        from app import get_db
        db = get_db()
        _create_solo_user(db)

        # A stronger user, so the solo user is not trivially rank #1.
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   ('bud149-bob', 'bobbud149'))
        db.execute('INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
                   ('bud149-bob', 4001, 99))
        db.commit()

        _log_in(client)

        # Primary navigation: the rail links the signed-in desk to the
        # leaderboard directly (not a fallback/error state).
        home = _render(client, '/home')
        assert '/leaderboard' in home, 'primary navigation does not link /leaderboard'

        content = _render(client, '/leaderboard')
        row = _standings_row(content)
        rank = int(re.search(r'class="pos-n">(\d+)<', row).group(1))
        assert 1 <= rank <= 2, f'solo user rank out of range: {rank}'
        assert _podium_score(row) == 30, 'solo user\'s global total_score is wrong'

        # The solo user is in no league: this is what makes them solo.
        assert db.execute(
            'SELECT COUNT(*) AS n FROM league_members WHERE user_id = ?',
            (SOLO_SID,)).fetchone()['n'] == 0


class TestNoLeagueGatingCopy:
    """AC2: no solo-reachable page gates the user on league membership."""

    def test_solo_user_pages_contain_no_league_gating_copy(self, app, client):
        from app import get_db
        db = get_db()
        _create_solo_user(db)
        _log_in(client)

        for path in ('/leaderboard', '/home', '/live'):
            content = _render(client, path)
            low = content.casefold()
            hits = [phrase for phrase in GATING_DENYLIST if phrase in low]
            assert not hits, f'{path} gates a solo user on league membership: {hits}'


class TestSoloScoreUnchangedWithLeagueTables:
    """AC3: league tables in the schema must not change a solo user's total."""

    def _solo_total(self, client):
        content = _render(client, '/leaderboard')
        return _podium_score(_standings_row(content))

    def test_solo_total_score_unchanged_after_league_tables(self, app, client):
        from app import get_db
        db = get_db()
        _create_solo_user(db)
        _log_in(client)

        # Before: no leagues, no memberships.
        before = self._solo_total(client)

        # After: the schema state later E3 stories (F1-20/F1-21/F1-25) create —
        # leagues and memberships exist and carry other users' points. The
        # solo user still has zero memberships.
        db.execute('''INSERT INTO leagues
            (id, name, emoji_or_color, start_round, whole_season, admin_user_id)
            VALUES (1, 'Rivals', '🏎', 1, 0, 'bud149-bob')''')
        db.execute("INSERT INTO league_members (league_id, user_id, is_admin) "
                   "VALUES (1, 'bud149-bob', 1), (1, 'bud149-alice', 0)")
        db.commit()

        assert db.execute(
            'SELECT COUNT(*) AS n FROM league_members WHERE user_id = ?',
            (SOLO_SID,)).fetchone()['n'] == 0

        after = self._solo_total(client)
        assert after == before == 30, \
            f'solo global total_score changed with league tables present: {before} -> {after}'


class TestStandingsQueryShape:
    """AC4: standings come from SUM(points) over `scores`, no league join."""

    def test_leaderboard_query_sums_scores_without_league_join(self, app, client, monkeypatch):
        from app import get_db
        db = get_db()
        _create_solo_user(db)
        _log_in(client)

        # Flask 3.x reuses the active app context for test-client requests,
        # so the request's connection IS this one: wrap get_db so the
        # capture proxy is what the route executes against.
        captured = []
        real = db

        def proxy_connect(database, *args, **kwargs):
            return _CapturingConnection(real, captured)

        class _Proxy:
            @staticmethod
            def execute(sql, *args, **kwargs):
                captured.append(sql)
                return real.execute(sql, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(real, name)

        monkeypatch.setattr(app_module, 'get_db', lambda: _Proxy())

        content = _render(client, '/leaderboard')
        assert _podium_score(_standings_row(content)) == 30

        queries = [q.strip() for q in captured]
        standings = [q for q in queries
                     if re.search(r'SUM\s*\(\s*(s\.)?points\s*\)', q, re.I)
                     and re.search(r'GROUP\s+BY', q, re.I)
                     and re.search(r'\busers\b', q, re.I)]
        assert standings, \
            'no SUM(points) ... GROUP BY query over users was issued for /leaderboard'

        for q in queries:
            assert not re.search(r'\bleagues?\b', q, re.I), \
                f'global standings path now references a league table: {q}'
            assert not re.search(r'\bleague_members\b', q, re.I), \
                f'global standings path now references league_members: {q}'
