"""Unit tests for The Wire's crowd-pick distribution.

Covers the two-sided chart data and the three states that used to collapse
into one indistinguishable empty result:

- ok           real rows to draw
- empty        query succeeded, nobody has submitted a card yet
- unavailable  query failed; the crowd is unknown, not empty

The distinction matters because a database error previously rendered as
"No cards in for this race yet", which reads as a quiet, wrong answer.
"""

import pytest

import app as app_module


class _BoomDB:
    """Stand-in database whose every query raises, to force the error path."""

    def execute(self, *args, **kwargs):
        raise RuntimeError('database is unavailable')


class TestPickDistributionStates:

    def test_failed_query_reports_unavailable_not_empty(self, app):
        result = app_module.get_pick_distribution(_BoomDB(), race_id=1)

        assert result['status'] == 'unavailable'
        assert result['rows'] == []
        assert result['total'] == 0

    def test_race_with_no_predictions_reports_empty(self, app):
        db = app_module.get_db()
        race_id = 8100
        db.execute(
            'INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
            (race_id, 'Empty GP', race_id, '2026-05-01 14:00:00', 'open'),
        )

        result = app_module.get_pick_distribution(db, race_id)

        # A race nobody has picked yet is 'empty', never 'unavailable'.
        assert result['status'] == 'empty'
        assert result['rows'] == []
        assert result['total'] == 0


class TestTwoSidedShares:
    """The chart draws passed_pct left of the midpoint and backed_pct right."""

    def _seed(self, db, race_id=8200):
        """Four cards; VER on every podium and winning two of them, ALO on one."""
        db.execute(
            'INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
            (race_id, 'Shares GP', race_id, '2026-05-02 14:00:00', 'locked'),
        )
        drivers = [
            (race_id + 1, 'VER', 'Max Verstappen'),
            (race_id + 2, 'HAM', 'Lewis Hamilton'),
            (race_id + 3, 'NOR', 'Lando Norris'),
            (race_id + 4, 'ALO', 'Fernando Alonso'),
        ]
        for did, code, name in drivers:
            db.execute(
                'INSERT INTO drivers (id, driver_id, name, team, number, code, nationality)'
                ' VALUES (?, ?, ?, ?, ?, ?, ?)',
                (did, f'{code.lower()}{race_id}', name, 'Team', did, code, 'NA'),
            )

        # p1 is the win pick; all three slots count as podium picks.
        cards = [
            (race_id + 1, race_id + 2, race_id + 3),
            (race_id + 1, race_id + 2, race_id + 3),
            (race_id + 2, race_id + 1, race_id + 3),
            (race_id + 2, race_id + 1, race_id + 4),
        ]
        for i, (p1, p2, p3) in enumerate(cards):
            sid = f'crowd-user-{race_id}-{i}'
            db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                       (sid, f'crowduser{race_id}{i}'))
            db.execute(
                'INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)'
                ' VALUES (?, ?, ?, ?, ?)',
                (sid, race_id, p1, p2, p3),
            )
        return race_id

    def test_known_shares(self, app):
        db = app_module.get_db()
        race_id = self._seed(db)

        result = app_module.get_pick_distribution(db, race_id)
        assert result['status'] == 'ok'
        assert result['total'] == 4

        by_code = {r['code']: r for r in result['rows']}

        # VER appears on all 4 cards and wins 2 of them.
        assert by_code['VER']['backed_pct'] == 100
        assert by_code['VER']['passed_pct'] == 0
        assert by_code['VER']['win_pct'] == 50

        # ALO appears on exactly 1 of 4 cards and never wins.
        assert by_code['ALO']['backed_pct'] == 25
        assert by_code['ALO']['passed_pct'] == 75
        assert by_code['ALO']['win_pct'] == 0

    def test_backed_and_passed_are_complementary(self, app):
        db = app_module.get_db()
        race_id = self._seed(db, race_id=8300)

        result = app_module.get_pick_distribution(db, race_id)
        assert result['status'] == 'ok'

        for driver in result['rows']:
            # Every card either backs the driver for the podium or does not,
            # which is what makes a two-sided bar honest without a sentiment feed.
            assert driver['backed_pct'] + driver['passed_pct'] == 100
            # Winning is a subset of making the podium.
            assert driver['win_pct'] <= driver['backed_pct']
            # podium_pct retained for existing callers.
            assert driver['podium_pct'] == driver['backed_pct']
