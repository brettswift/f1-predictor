"""BUD-156 / F1-26: league admin basics — rename, remove member, transfer
admin, delete league, leave league (including sole-admin-leaves).

Epic E3, Phase 1. A league is purely a view over the global game (§3.5):
none of these admin actions may touch `predictions`, `scores`, `races`, or
`results`, and none may change any member's global leaderboard total.
Deletion safety is the highest-risk part of this story, so it is covered
first.

Acceptance criteria covered (see Linear BUD-156):
  AC1  Rename: membership/predictions/scores row counts unchanged.
  AC2  Remove member: member's league_members row gone; their
       predictions/scores rows and global leaderboard total unchanged.
  AC3  Transfer admin: ex-admin's admin-only actions rejected after
       transfer; new admin's accepted - tested both ways.
  AC4  Delete league: only leagues/league_members rows removed; zero rows
       removed from predictions/scores/races/results; every former
       member's global leaderboard total unchanged.
  AC5  Leave league (including sole-admin-leaves): admin seat
       auto-transfers to the longest-tenured other member before the
       leaving admin's membership is removed; league is never left
       admin-less while members remain.
  AC6  All of the above are self-serve via routes - no direct DB
       intervention required (exercised via the Flask test client).
"""

import pytest

from app import (
    get_db, get_user_leagues, is_league_admin, is_league_member,
    remove_league_member, rename_league, transfer_league_admin,
    delete_league, reassign_admin_before_leaving,
)

RACE_ID = 1
RACE_DATE = '2026-03-08 14:00:00'

ADMIN = 'bud156-admin'
MEMBER = 'bud156-member'
MEMBER2 = 'bud156-member2'
OUTSIDER = 'bud156-outsider'

DRIVERS = (1, 2, 3, 4, 5, 6)
PICK = (1, 2, 3)
POINTS = 20  # exact podium: 10 + 6 + 4

LEAGUE_NAME = 'BUD156 League'


def _login(client, session_id):
    with client.session_transaction() as sess:
        sess['session_id'] = session_id


def _insert_race(db):
    db.execute(
        'INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
        (RACE_ID, 'BUD156 GP', 1, RACE_DATE, 'completed'))


def _insert_drivers(db):
    for driver_id, number in enumerate(DRIVERS, start=1):
        db.execute(
            'INSERT INTO drivers (id, driver_id, name, number, code) '
            'VALUES (?, ?, ?, ?, ?)',
            (driver_id, f'b156d{driver_id}', f'BUD156 Driver {driver_id}',
             number, f'B16{number}'))


def _insert_user(db, session_id):
    db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
               (session_id, session_id.split('-')[-1]))


def _score(db, session_id, points=POINTS):
    """Give a user exactly one predictions row and one scores row for RACE_ID."""
    db.execute(
        'INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) '
        'VALUES (?, ?, ?, ?, ?)', (session_id, RACE_ID) + PICK)
    db.execute('INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
               (session_id, RACE_ID, points))


def _league_id(db):
    return db.execute('SELECT id FROM leagues WHERE name = ?', (LEAGUE_NAME,)).fetchone()['id']


def _global_total(db, session_id):
    return db.execute(
        'SELECT COALESCE(SUM(points), 0) AS total FROM scores WHERE user_id = ?',
        (session_id,)).fetchone()['total']


def _counts(db):
    """(predictions, scores, races, results) row counts - the tables that
    must never move for any admin action in this story."""
    return tuple(
        db.execute(f'SELECT COUNT(*) AS n FROM {t}').fetchone()['n']
        for t in ('predictions', 'scores', 'races', 'results')
    )


@pytest.fixture
def world(app, client):
    """1 completed race, 1 league: ADMIN (admin), MEMBER, MEMBER2 (both
    plain members, MEMBER joined first). All three have scored points so
    row-count and leaderboard-total invariants have something to break."""
    db = get_db()

    _insert_race(db)
    _insert_drivers(db)
    for uid in (ADMIN, MEMBER, MEMBER2, OUTSIDER):
        _insert_user(db, uid)

    db.execute(
        '''INSERT INTO leagues (name, emoji_or_color, start_round, whole_season, admin_user_id)
           VALUES (?, ?, 1, 0, ?)''',
        (LEAGUE_NAME, '\U0001F3CE', ADMIN))
    league_id = _league_id(db)
    db.execute(
        '''INSERT INTO league_members (league_id, user_id, joined_at_round, is_admin, joined_at)
           VALUES (?, ?, 1, 1, '2026-01-01 00:00:00')''',
        (league_id, ADMIN))
    db.execute(
        '''INSERT INTO league_members (league_id, user_id, joined_at_round, is_admin, joined_at)
           VALUES (?, ?, 1, 0, '2026-01-02 00:00:00')''',
        (league_id, MEMBER))
    db.execute(
        '''INSERT INTO league_members (league_id, user_id, joined_at_round, is_admin, joined_at)
           VALUES (?, ?, 1, 0, '2026-01-03 00:00:00')''',
        (league_id, MEMBER2))

    _score(db, ADMIN)
    _score(db, MEMBER)
    _score(db, MEMBER2)
    # Outsider: has points, belongs to no league - must never be touched.
    _score(db, OUTSIDER)
    db.commit()
    return db


# ── AC4: delete safety comes first - the highest-risk part of the story ────

class TestDeleteLeagueNeverTouchesGlobalHistory:
    def test_delete_removes_zero_rows_from_global_tables(self, app, client, world):
        db = world
        league_id = _league_id(db)
        before = _counts(db)
        before_totals = {u: _global_total(db, u) for u in (ADMIN, MEMBER, MEMBER2, OUTSIDER)}

        delete_league(db, league_id)

        assert _counts(db) == before, \
            'deleting a league must remove zero rows from predictions/scores/races/results'
        for u, total in before_totals.items():
            assert _global_total(db, u) == total, f'{u} global total changed after league delete'

    def test_delete_removes_league_and_membership_rows(self, app, client, world):
        db = world
        league_id = _league_id(db)

        delete_league(db, league_id)

        assert db.execute('SELECT 1 FROM leagues WHERE id = ?', (league_id,)).fetchone() is None
        assert db.execute(
            'SELECT COUNT(*) AS n FROM league_members WHERE league_id = ?',
            (league_id,)).fetchone()['n'] == 0

    def test_delete_route_is_admin_only_and_self_serve(self, app, client, world):
        db = world
        league_id = _league_id(db)

        _login(client, MEMBER)
        resp = client.post(f'/leagues/{league_id}/delete', follow_redirects=True)
        assert resp.status_code == 200
        assert db.execute('SELECT 1 FROM leagues WHERE id = ?', (league_id,)).fetchone() is not None, \
            'non-admin must not be able to delete the league'

        _login(client, ADMIN)
        resp = client.post(f'/leagues/{league_id}/delete', follow_redirects=True)
        assert resp.status_code == 200
        assert db.execute('SELECT 1 FROM leagues WHERE id = ?', (league_id,)).fetchone() is None


# ── AC1: rename ─────────────────────────────────────────────────────────────

class TestRenameLeague:
    def test_rename_leaves_membership_predictions_scores_untouched(self, app, client, world):
        db = world
        league_id = _league_id(db)
        before_members = db.execute(
            'SELECT COUNT(*) AS n FROM league_members WHERE league_id = ?',
            (league_id,)).fetchone()['n']
        before = _counts(db)

        rename_league(db, league_id, 'BUD156 Renamed')

        assert db.execute('SELECT name FROM leagues WHERE id = ?', (league_id,)).fetchone()['name'] \
            == 'BUD156 Renamed'
        assert db.execute(
            'SELECT COUNT(*) AS n FROM league_members WHERE league_id = ?',
            (league_id,)).fetchone()['n'] == before_members
        assert _counts(db) == before

    def test_rename_route_is_admin_only(self, app, client, world):
        db = world
        league_id = _league_id(db)

        _login(client, MEMBER)
        client.post(f'/leagues/{league_id}/rename', data={'name': 'Hijacked'}, follow_redirects=True)
        assert db.execute('SELECT name FROM leagues WHERE id = ?', (league_id,)).fetchone()['name'] \
            == LEAGUE_NAME

        _login(client, ADMIN)
        resp = client.post(f'/leagues/{league_id}/rename', data={'name': 'BUD156 Renamed'},
                            follow_redirects=True)
        assert resp.status_code == 200
        assert db.execute('SELECT name FROM leagues WHERE id = ?', (league_id,)).fetchone()['name'] \
            == 'BUD156 Renamed'


# ── AC2: remove member ───────────────────────────────────────────────────────

class TestRemoveMember:
    def test_remove_member_untouched_predictions_scores_and_leaderboard(self, app, client, world):
        db = world
        league_id = _league_id(db)
        before = _counts(db)
        before_total = _global_total(db, MEMBER)

        remove_league_member(db, league_id, MEMBER)

        assert not is_league_member(db, league_id, MEMBER)
        assert _counts(db) == before
        assert _global_total(db, MEMBER) == before_total

        _login(client, MEMBER)
        html = client.get('/leaderboard?season=current').data.decode('utf-8')
        assert str(before_total) in html or f'<span class="pts">{before_total}</span>' in html

    def test_remove_member_route_is_admin_only(self, app, client, world):
        db = world
        league_id = _league_id(db)

        _login(client, MEMBER2)
        client.post(f'/leagues/{league_id}/remove-member', data={'user_id': MEMBER},
                    follow_redirects=True)
        assert is_league_member(db, league_id, MEMBER), \
            'non-admin must not be able to remove another member'

        _login(client, ADMIN)
        resp = client.post(f'/leagues/{league_id}/remove-member', data={'user_id': MEMBER},
                            follow_redirects=True)
        assert resp.status_code == 200
        assert not is_league_member(db, league_id, MEMBER)

    def test_admin_cannot_remove_self_via_this_route(self, app, client, world):
        db = world
        league_id = _league_id(db)

        _login(client, ADMIN)
        client.post(f'/leagues/{league_id}/remove-member', data={'user_id': ADMIN},
                    follow_redirects=True)
        assert is_league_member(db, league_id, ADMIN), \
            'remove-member must not double as a self-leave route'


# ── AC3: transfer admin, tested both ways ────────────────────────────────────

class TestTransferAdmin:
    def test_transfer_flips_is_admin_flag(self, app, client, world):
        db = world
        league_id = _league_id(db)

        transfer_league_admin(db, league_id, ADMIN, MEMBER)

        assert not is_league_admin(db, league_id, ADMIN)
        assert is_league_admin(db, league_id, MEMBER)

    def test_ex_admin_rejected_new_admin_accepted_for_all_three_actions(self, app, client, world):
        db = world
        league_id = _league_id(db)

        _login(client, ADMIN)
        client.post(f'/leagues/{league_id}/transfer-admin', data={'user_id': MEMBER},
                    follow_redirects=True)
        assert is_league_admin(db, league_id, MEMBER)
        assert not is_league_admin(db, league_id, ADMIN)

        # Ex-admin (ADMIN) rejected on all three admin-only actions.
        _login(client, ADMIN)
        client.post(f'/leagues/{league_id}/rename', data={'name': 'Should not stick'},
                    follow_redirects=True)
        assert db.execute('SELECT name FROM leagues WHERE id = ?', (league_id,)).fetchone()['name'] \
            == LEAGUE_NAME
        client.post(f'/leagues/{league_id}/remove-member', data={'user_id': MEMBER2},
                    follow_redirects=True)
        assert is_league_member(db, league_id, MEMBER2)
        client.post(f'/leagues/{league_id}/delete', follow_redirects=True)
        assert db.execute('SELECT 1 FROM leagues WHERE id = ?', (league_id,)).fetchone() is not None

        # New admin (MEMBER) accepted on all three.
        _login(client, MEMBER)
        resp = client.post(f'/leagues/{league_id}/rename', data={'name': 'New Admin Renamed'},
                            follow_redirects=True)
        assert resp.status_code == 200
        assert db.execute('SELECT name FROM leagues WHERE id = ?', (league_id,)).fetchone()['name'] \
            == 'New Admin Renamed'
        resp = client.post(f'/leagues/{league_id}/remove-member', data={'user_id': MEMBER2},
                            follow_redirects=True)
        assert resp.status_code == 200
        assert not is_league_member(db, league_id, MEMBER2)
        resp = client.post(f'/leagues/{league_id}/delete', follow_redirects=True)
        assert resp.status_code == 200
        assert db.execute('SELECT 1 FROM leagues WHERE id = ?', (league_id,)).fetchone() is None

    def test_transfer_route_rejects_non_admin_caller(self, app, client, world):
        db = world
        league_id = _league_id(db)

        _login(client, MEMBER)
        client.post(f'/leagues/{league_id}/transfer-admin', data={'user_id': MEMBER2},
                    follow_redirects=True)
        assert is_league_admin(db, league_id, ADMIN)
        assert not is_league_admin(db, league_id, MEMBER2)


# ── AC5: leave league, including the sole-admin-leaves edge case ───────────

class TestLeaveLeague:
    def test_non_admin_member_leaves_admin_unaffected(self, app, client, world):
        db = world
        league_id = _league_id(db)

        _login(client, MEMBER)
        resp = client.post(f'/leagues/{league_id}/leave', follow_redirects=True)
        assert resp.status_code == 200

        assert not is_league_member(db, league_id, MEMBER)
        assert is_league_admin(db, league_id, ADMIN)

    def test_sole_admin_leaving_auto_transfers_to_longest_tenured_member(self, app, client, world):
        """MEMBER joined before MEMBER2 (see fixture), so when the admin
        leaves, MEMBER - not MEMBER2 - inherits the seat, and the league is
        never left without an admin while members remain."""
        db = world
        league_id = _league_id(db)

        _login(client, ADMIN)
        resp = client.post(f'/leagues/{league_id}/leave', follow_redirects=True)
        assert resp.status_code == 200

        assert not is_league_member(db, league_id, ADMIN)
        assert is_league_admin(db, league_id, MEMBER), \
            'admin seat must auto-transfer to the longest-tenured remaining member'
        assert not is_league_admin(db, league_id, MEMBER2)

        remaining = db.execute(
            'SELECT COUNT(*) AS n FROM league_members WHERE league_id = ?',
            (league_id,)).fetchone()['n']
        assert remaining == 2
        admins = db.execute(
            'SELECT COUNT(*) AS n FROM league_members WHERE league_id = ? AND is_admin = 1',
            (league_id,)).fetchone()['n']
        assert admins == 1, 'league must have exactly one admin while members remain'

    def test_sole_admin_and_sole_member_leaving_empties_league_without_error(self, app, client, world):
        """No other members to inherit the seat: the league simply ends up
        with zero members - not the forbidden 'admin-less with members
        present' state - and predictions/scores are untouched."""
        db = world
        league_id = _league_id(db)
        db.execute('DELETE FROM league_members WHERE league_id = ? AND user_id != ?',
                   (league_id, ADMIN))
        db.commit()
        before = _counts(db)

        _login(client, ADMIN)
        resp = client.post(f'/leagues/{league_id}/leave', follow_redirects=True)
        assert resp.status_code == 200

        remaining = db.execute(
            'SELECT COUNT(*) AS n FROM league_members WHERE league_id = ?',
            (league_id,)).fetchone()['n']
        assert remaining == 0
        assert db.execute('SELECT 1 FROM leagues WHERE id = ?', (league_id,)).fetchone() is not None
        assert _counts(db) == before

    def test_reassign_admin_before_leaving_is_noop_for_non_admin(self, app, client, world):
        db = world
        league_id = _league_id(db)
        reassign_admin_before_leaving(db, league_id, MEMBER)
        assert is_league_admin(db, league_id, ADMIN)
        assert not is_league_admin(db, league_id, MEMBER)
