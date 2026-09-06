"""BUD-158 / F1-28: "joined at round X" context in league tables.

Epic E3 ("leagues as views over the global game"), Phase 1. F1-25 added
`league_members.joined_at`; this story reads it and displays the round the
member joined at, so a late joiner's smaller scored-race count is visibly
explained on the league table itself.

Acceptance criteria covered (see Linear BUD-158):
  AC1  League detail page shows, per member, the round they joined at.
  AC2  The value is computed from `league_members.joined_at` against the
       `races` table (first race whose date >= joined_at) - not the stored
       `joined_at_round` heuristic column, not earliest-prediction -
       verified against the DB row directly, not just a fixed expectation.
  AC3  The league creator (admin) shows "round 1", consistent with F1-20's
       create flow, regardless of when they view the page.
  AC4  The join-round column is visible to every member (not admin-only)
       and lives in the league table itself (leaderboard, scoped by
       ?league=) alongside rank/name/score - not relegated to the
       members-only tab.
"""

import re

import pytest

from app import get_db, get_member_joined_round, get_league_members_with_joined_round

RACE_DATES = {
    1: '2026-03-08 14:00:00',
    2: '2026-03-22 14:00:00',
    3: '2026-04-05 14:00:00',
}

ADMIN = 'bud158-admin'      # creates the league - must show round 1
EARLY = 'bud158-early'      # joins before round 1's race - round 1
MID = 'bud158-mid'          # joins between round 1 and round 2 - round 2
LATE = 'bud158-late'        # joins after every scheduled race - falls back to last round

JOINED_AT = {
    ADMIN: '2026-01-01 00:00:00',   # league-create timestamp, well before R1
    EARLY: '2026-03-01 00:00:00',   # before R1's date
    MID: '2026-03-15 00:00:00',     # after R1, before R2
    LATE: '2026-04-20 00:00:00',    # after R3, the last scheduled race
}

EXPECTED_ROUND = {ADMIN: 1, EARLY: 1, MID: 2, LATE: 3}

LEAGUE_NAME = 'BUD158 League'


def _username(session_id):
    return session_id.split('-')[-1]


def _login(client, session_id):
    with client.session_transaction() as sess:
        sess['session_id'] = session_id


def _insert_race(db, race_id, round_num):
    db.execute(
        'INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
        (race_id, f'BUD158 GP {round_num}', round_num, RACE_DATES[round_num], 'completed'))


def _insert_user(db, session_id):
    db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
               (session_id, _username(session_id)))


def _league_id(db):
    return db.execute('SELECT id FROM leagues WHERE name = ?', (LEAGUE_NAME,)).fetchone()['id']


def _row_for(html, needle):
    """The `<tr...` chunk containing `needle`, or None."""
    for chunk in html.split('<tr'):
        if needle in chunk:
            return chunk
    return None


@pytest.fixture
def world(app, client):
    """3 races, 1 league: an admin (round 1), an early joiner (round 1), a
    mid-season joiner (round 2) and a joiner after every scheduled race
    (falls back to the last round)."""
    db = get_db()

    for round_num in (1, 2, 3):
        _insert_race(db, round_num, round_num)
    for sid in (ADMIN, EARLY, MID, LATE):
        _insert_user(db, sid)

    db.execute(
        '''INSERT INTO leagues (name, emoji_or_color, start_round, whole_season, admin_user_id)
           VALUES (?, ?, 1, 0, ?)''',
        (LEAGUE_NAME, '\U0001F3CE', ADMIN))
    league_id = _league_id(db)
    for sid, is_admin in ((ADMIN, 1), (EARLY, 0), (MID, 0), (LATE, 0)):
        db.execute(
            '''INSERT INTO league_members (league_id, user_id, joined_at_round, is_admin, joined_at)
               VALUES (?, ?, 99, ?, ?)''',
            # joined_at_round is deliberately seeded with a wrong value (99)
            # to prove the display path never reads that column.
            (league_id, sid, is_admin, JOINED_AT[sid]))
    db.commit()
    return db


# ── AC2: pure date-bracket computation, verified against the DB row ─────────

class TestGetMemberJoinedRound:
    def test_matches_expected_round_for_each_joined_at(self, app, client, world):
        db = world
        for sid, expected in EXPECTED_ROUND.items():
            assert get_member_joined_round(db, JOINED_AT[sid]) == expected, \
                f'{sid} joined_at={JOINED_AT[sid]!r} expected round {expected}'

    def test_ignores_the_stale_joined_at_round_column(self, app, client, world):
        """Fixture seeds joined_at_round=99 for everyone - if the read path
        used that column instead of joined_at, every result would be 99."""
        db = world
        league_id = _league_id(db)
        members = get_league_members_with_joined_round(db, league_id)
        assert all(m['joined_at_round'] == 99 for m in members), 'fixture sanity check'
        assert all(m['joined_round'] != 99 for m in members)

    def test_joined_round_matches_the_db_row_for_league_and_member(self, app, client, world):
        """Directly verifies AC2: recompute from the actual league_members
        row (league_id + user_id), not from the fixture's expected-value
        table, and confirm it lines up."""
        db = world
        league_id = _league_id(db)
        for sid in (ADMIN, EARLY, MID, LATE):
            row = db.execute(
                'SELECT joined_at FROM league_members WHERE league_id = ? AND user_id = ?',
                (league_id, sid)).fetchone()
            assert get_member_joined_round(db, row['joined_at']) == EXPECTED_ROUND[sid]


# ── AC3: admin shows round 1 regardless of when they view the page ──────────

class TestAdminShowsRoundOne:
    def test_admin_joined_round_is_one(self, app, client, world):
        db = world
        league_id = _league_id(db)
        members = {m['user_id']: m for m in get_league_members_with_joined_round(db, league_id)}
        assert members[ADMIN]['is_admin'] == 1
        assert members[ADMIN]['joined_round'] == 1

    def test_admin_sees_round_one_no_matter_which_page_loads_first(self, app, client, world):
        """Load order must not matter - hitting /leaderboard before
        /leagues/<id> (or vice versa) must not change the computed round."""
        db = world
        league_id = _league_id(db)
        _login(client, ADMIN)

        client.get(f'/leaderboard?league={league_id}')
        first = _row_for(
            client.get(f'/leagues/{league_id}').data.decode('utf-8'), f'>{ADMIN}<')
        assert first is not None and 'Round 1' in first


# ── AC1 / AC4: visible to every member, in the league table itself ──────────

class TestLeagueDetailPageShowsJoinedRound:
    def test_every_member_sees_every_joined_round_not_just_the_admin(self, app, client, world):
        db = world
        league_id = _league_id(db)

        for viewer in (ADMIN, EARLY, MID, LATE):
            _login(client, viewer)
            html = client.get(f'/leagues/{league_id}').data.decode('utf-8')
            for sid, expected in EXPECTED_ROUND.items():
                row = _row_for(html, f'>{sid}<')
                assert row is not None, f'{sid} missing from members table as seen by {viewer}'
                assert f'Round {expected}' in row, \
                    f'{viewer} should see {sid} at Round {expected}, row: {row!r}'


class TestLeaderboardLeagueScopedShowsJoinedRound:
    """The join-round column must sit in the actual league table (rank/name/
    score), not only the separate members tab - AC4."""

    def test_league_scoped_leaderboard_has_joined_round_per_member(self, app, client, world):
        db = world
        league_id = _league_id(db)
        _login(client, EARLY)

        html = client.get(f'/leaderboard?league={league_id}').data.decode('utf-8')
        assert LEAGUE_NAME in html
        for sid, expected in EXPECTED_ROUND.items():
            row = _row_for(html, f'>{_username(sid)}<')
            assert row is not None, f'{sid} missing from league-scoped standings table'
            m = re.search(r'>R(\d+)</span></td>', row)
            assert m is not None, f'no joined-round cell rendered for {sid}: {row!r}'
            assert int(m.group(1)) == expected, \
                f'{sid}: rendered R{m.group(1)}, expected R{expected}'

    def test_global_leaderboard_has_no_joined_round_column(self, app, client, world):
        """Without a league scope there is no join-round concept - the
        column must not appear on the global view."""
        db = world
        _login(client, EARLY)
        html = client.get('/leaderboard?season=current').data.decode('utf-8')
        assert '<th style="width:76px">Joined</th>' not in html
