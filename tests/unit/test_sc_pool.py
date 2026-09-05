"""Unit tests for the safety-car pool (BUD-139 / F1-31).

Covers the gap flagged on BUD-139: sc_multiplier(), get_sc_crowd(),
get_sc_pool(), get_sc_pools(), and the pool-floors-at-zero /
round-order-replay settlement logic in _sc_settled(). These functions were
shipped as part of the Undercut UI rewrite (2026-09-05) with no test
coverage; this fills that in without changing any of the behavior.
"""
import pytest
from datetime import datetime, timezone, timedelta


def _insert_race(db, round_num, name=None):
    """Insert a race, return its id."""
    race_time = datetime.now(timezone.utc) - timedelta(days=1)
    db.execute(
        'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
        (name or f'Round {round_num} GP', round_num,
         race_time.strftime('%Y-%m-%d %H:%M:%S'), 'completed')
    )
    db.commit()
    return db.execute('SELECT id FROM races WHERE round = ?', (round_num,)).fetchone()['id']


def _ensure_dummy_drivers(db):
    """SC logic never reads driver identity, but results.p1/p2/p3_driver_id are
    NOT NULL - insert three placeholder drivers once and reuse their ids."""
    row = db.execute("SELECT id FROM drivers WHERE driver_id = 'sc-test-dummy-1'").fetchone()
    if row:
        ids = [
            db.execute("SELECT id FROM drivers WHERE driver_id = ?", (f'sc-test-dummy-{i}',)).fetchone()['id']
            for i in (1, 2, 3)
        ]
        return ids
    ids = []
    for i in (1, 2, 3):
        db.execute(
            'INSERT INTO drivers (driver_id, name, number, code) VALUES (?, ?, ?, ?)',
            (f'sc-test-dummy-{i}', f'Dummy Driver {i}', 900 + i, f'D{i}')
        )
        ids.append(db.execute("SELECT id FROM drivers WHERE driver_id = ?", (f'sc-test-dummy-{i}',)).fetchone()['id'])
    db.commit()
    return ids


def _insert_result(db, race_id, had_sc=0, had_vsc=0):
    """Insert a result row with safety-car flags for a race (drivers unused by SC logic)."""
    p1, p2, p3 = _ensure_dummy_drivers(db)
    db.execute(
        '''INSERT INTO results (race_id, p1_driver_id, p2_driver_id, p3_driver_id,
                                 had_safety_car, had_virtual_safety_car)
           VALUES (?, ?, ?, ?, ?, ?)''',
        (race_id, p1, p2, p3, had_sc, had_vsc)
    )
    db.commit()


def _insert_sc_vote(db, user_id, race_id, conviction, multiplier):
    db.execute(
        'INSERT INTO sc_votes (user_id, race_id, conviction, multiplier) VALUES (?, ?, ?, ?)',
        (user_id, race_id, conviction, multiplier)
    )
    db.commit()


class TestScMultiplier:
    """sc_multiplier(conviction, consensus) - prices a call against the crowd."""

    def test_below_min_stake_is_free(self, app):
        from app import sc_multiplier, SC_MIN_STAKE
        assert abs(5) < SC_MIN_STAKE  # sanity: 5 is below the 6-point floor
        assert sc_multiplier(5, 0) == 1.0
        assert sc_multiplier(-5, 40) == 1.0
        assert sc_multiplier(0, 100) == 1.0

    def test_at_min_stake_is_priced_not_free(self, app):
        from app import sc_multiplier, SC_MIN_STAKE
        result = sc_multiplier(SC_MIN_STAKE, 0)
        assert result != 1.0, "a stake exactly at the minimum should be priced, not treated as no-op"

    def test_matches_crowd_consensus_prices_lowest(self, app):
        """Calling exactly with the crowd (distance=0) should price below a contrarian call."""
        from app import sc_multiplier
        with_crowd = sc_multiplier(60, 60)
        against_crowd = sc_multiplier(-60, 60)
        assert with_crowd < against_crowd

    def test_contrarian_call_pays_more_the_further_from_crowd(self, app):
        from app import sc_multiplier
        near = sc_multiplier(10, 0)
        far = sc_multiplier(100, -100)
        assert far > near

    def test_negative_and_positive_conviction_symmetric_at_zero_consensus(self, app):
        from app import sc_multiplier
        assert sc_multiplier(80, 0) == sc_multiplier(-80, 0)

    def test_formula_matches_documented_shape(self, app):
        """1 + distance/200*3.4 + |conviction|/260, rounded to 2 places."""
        from app import sc_multiplier
        conviction, consensus = 50, -20
        distance = abs(conviction - consensus) / 200.0
        expected = round(1.0 + distance * 3.4 + abs(conviction) / 260.0, 2)
        assert sc_multiplier(conviction, consensus) == expected


class TestScSettled:
    """_sc_settled(conviction, had_sc) - did the call match the race."""

    def test_safety_car_call_wins_when_sc_happened(self, app):
        from app import _sc_settled
        assert _sc_settled(70, True) is True

    def test_safety_car_call_loses_when_no_sc(self, app):
        from app import _sc_settled
        assert _sc_settled(70, False) is False

    def test_clean_race_call_wins_when_no_sc(self, app):
        from app import _sc_settled
        assert _sc_settled(-70, False) is True

    def test_clean_race_call_loses_when_sc_happened(self, app):
        from app import _sc_settled
        assert _sc_settled(-70, True) is False

    def test_had_sc_accepts_truthy_int_not_just_bool(self, app):
        """Callers pass sqlite ints (0/1), not Python bools."""
        from app import _sc_settled
        assert _sc_settled(50, 1) is True
        assert _sc_settled(50, 0) is False


class TestScCrowd:
    """get_sc_crowd(db, race_id) - the field's own calls for a race."""

    def test_no_votes_defaults_to_neutral(self, app):
        from app import get_db, get_sc_crowd
        db = get_db()
        race_id = _insert_race(db, 201)
        crowd = get_sc_crowd(db, race_id)
        assert crowd == {'votes': 0, 'yes_pct': 50, 'consensus': 0}

    def test_all_yes_votes(self, app):
        from app import get_db, get_sc_crowd
        db = get_db()
        race_id = _insert_race(db, 202)
        _insert_sc_vote(db, 'u1', race_id, 80, 1.5)
        _insert_sc_vote(db, 'u2', race_id, 40, 1.2)
        crowd = get_sc_crowd(db, race_id)
        assert crowd == {'votes': 2, 'yes_pct': 100, 'consensus': 100}

    def test_all_no_votes(self, app):
        from app import get_db, get_sc_crowd
        db = get_db()
        race_id = _insert_race(db, 203)
        _insert_sc_vote(db, 'u1', race_id, -80, 1.5)
        crowd = get_sc_crowd(db, race_id)
        assert crowd == {'votes': 1, 'yes_pct': 0, 'consensus': -100}

    def test_mixed_votes_consensus_formula(self, app):
        """3 yes, 1 no -> yes_pct=75, consensus = 75*2-100 = 50."""
        from app import get_db, get_sc_crowd
        db = get_db()
        race_id = _insert_race(db, 204)
        for i, conv in enumerate([80, 60, 30, -50]):
            _insert_sc_vote(db, f'u{i}', race_id, conv, 1.3)
        crowd = get_sc_crowd(db, race_id)
        assert crowd == {'votes': 4, 'yes_pct': 75, 'consensus': 50}

    def test_votes_scoped_to_race(self, app):
        """A vote on a different race must not leak into this race's crowd."""
        from app import get_db, get_sc_crowd
        db = get_db()
        race_a = _insert_race(db, 205)
        race_b = _insert_race(db, 206)
        _insert_sc_vote(db, 'u1', race_a, 90, 1.5)
        crowd_b = get_sc_crowd(db, race_b)
        assert crowd_b == {'votes': 0, 'yes_pct': 50, 'consensus': 0}


class TestScPool:
    """get_sc_pool(db, user_id) - one player's pool, replayed in round order."""

    def test_no_user_id_returns_starting_pool(self, app):
        from app import get_db, get_sc_pool, SC_POOL_START
        db = get_db()
        result = get_sc_pool(db, None)
        assert result == {'pool': SC_POOL_START, 'settled': 0, 'won': 0}

    def test_user_with_no_calls_has_starting_pool(self, app):
        from app import get_db, get_sc_pool, SC_POOL_START
        db = get_db()
        result = get_sc_pool(db, 'nobody-ever-called')
        assert result == {'pool': SC_POOL_START, 'settled': 0, 'won': 0}

    def test_single_winning_call_adds_the_swing(self, app):
        from app import get_db, get_sc_pool, SC_POOL_START
        db = get_db()
        race_id = _insert_race(db, 210)
        _insert_result(db, race_id, had_sc=1)
        _insert_sc_vote(db, 'winner', race_id, conviction=50, multiplier=2.0)
        result = get_sc_pool(db, 'winner')
        assert result['pool'] == SC_POOL_START + round(50 * 2.0)
        assert result['settled'] == 1
        assert result['won'] == 1

    def test_single_losing_call_subtracts_the_swing(self, app):
        from app import get_db, get_sc_pool, SC_POOL_START
        db = get_db()
        race_id = _insert_race(db, 211)
        _insert_result(db, race_id, had_sc=0)
        _insert_sc_vote(db, 'loser', race_id, conviction=30, multiplier=1.5)
        result = get_sc_pool(db, 'loser')
        assert result['pool'] == SC_POOL_START - round(30 * 1.5)
        assert result['won'] == 0

    def test_pool_floors_at_zero_never_goes_negative(self, app):
        """A stake big enough to wipe out the whole pool clamps at 0, not negative."""
        from app import get_db, get_sc_pool
        db = get_db()
        race_id = _insert_race(db, 212)
        _insert_result(db, race_id, had_sc=0)  # 'clean' happened, call was wrong
        # conviction=100 * multiplier=3.0 = 300 swing, far more than the 100-point pool
        _insert_sc_vote(db, 'wipeout', race_id, conviction=100, multiplier=3.0)
        result = get_sc_pool(db, 'wipeout')
        assert result['pool'] == 0

    def test_settlement_replays_in_round_order_not_insertion_order(self, app):
        """Insert a later round's vote first; the pool must still replay by round."""
        from app import get_db, get_sc_pool, SC_POOL_START
        db = get_db()
        race_later = _insert_race(db, 220)  # round 220, inserted first
        race_earlier = _insert_race(db, 215)  # round 215, inserted second

        # A win in the earlier round, then a big enough loss in the later round
        # to floor the pool - only correct if replay honors round order.
        _insert_result(db, race_later, had_sc=0)
        _insert_sc_vote(db, 'order-matters', race_later, conviction=100, multiplier=3.0)
        _insert_result(db, race_earlier, had_sc=1)
        _insert_sc_vote(db, 'order-matters', race_earlier, conviction=20, multiplier=1.0)

        result = get_sc_pool(db, 'order-matters')
        # Correct order: start 100 -> +20 (round 215 win) = 120 -> -300 (round 220 loss) floors at 0.
        assert result['pool'] == 0
        assert result['settled'] == 2
        assert result['won'] == 1

    def test_pool_isolated_per_user(self, app):
        from app import get_db, get_sc_pool, SC_POOL_START
        db = get_db()
        race_id = _insert_race(db, 225)
        _insert_result(db, race_id, had_sc=1)
        _insert_sc_vote(db, 'player-a', race_id, conviction=40, multiplier=1.5)
        # player-b never called - must be untouched by player-a's win.
        result_b = get_sc_pool(db, 'player-b')
        assert result_b['pool'] == SC_POOL_START


class TestScPools:
    """get_sc_pools(db) - every player's pool in one pass, for standings."""

    def test_empty_when_no_votes_exist(self, app):
        from app import get_db, get_sc_pools
        db = get_db()
        assert get_sc_pools(db) == {}

    def test_matches_get_sc_pool_per_user(self, app):
        """The batch version must agree with the single-user version."""
        from app import get_db, get_sc_pool, get_sc_pools, SC_POOL_START
        db = get_db()
        race_1 = _insert_race(db, 230)
        race_2 = _insert_race(db, 231)
        _insert_result(db, race_1, had_sc=1)
        _insert_result(db, race_2, had_sc=0)
        _insert_sc_vote(db, 'alice', race_1, conviction=60, multiplier=1.8)
        _insert_sc_vote(db, 'alice', race_2, conviction=-30, multiplier=1.2)
        _insert_sc_vote(db, 'bob', race_1, conviction=-60, multiplier=1.8)

        pools = get_sc_pools(db)
        assert pools['alice'] == get_sc_pool(db, 'alice')['pool']
        assert pools['bob'] == get_sc_pool(db, 'bob')['pool']

    def test_floor_at_zero_applies_per_user_in_batch(self, app):
        from app import get_db, get_sc_pools
        db = get_db()
        race_id = _insert_race(db, 235)
        _insert_result(db, race_id, had_sc=0)
        _insert_sc_vote(db, 'busted', race_id, conviction=100, multiplier=5.0)
        pools = get_sc_pools(db)
        assert pools['busted'] == 0
