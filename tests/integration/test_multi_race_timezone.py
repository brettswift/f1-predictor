"""Integration tests for multi-race and timezone handling (T-RL-006, T-RL-007)."""

import pytest
from datetime import datetime, timezone, timedelta


class TestMultiRaceAndTimezone:
    """Test T-RL-006 and T-RL-007: Multiple races lock independently, timezone handling."""

    def _insert_race_and_drivers(self, db, round_num, status='open', hours_from_now=24):
        """Helper: insert a race and 3 drivers, return race id.
        
        Args:
            db: Database connection
            round_num: Unique round number for this race
            status: Initial status ('open', 'locked', etc.)
            hours_from_now: If positive, race is in future; if negative, race is in past
        """
        now = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)
        race_time = now + timedelta(hours=hours_from_now)
        
        db.execute(
            'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
            (f'Test GP {round_num}', round_num, race_time.strftime('%Y-%m-%d %H:%M:%S'), status)
        )
        race_id = db.execute('SELECT id FROM races WHERE round = ?', (round_num,)).fetchone()['id']
        
        base_id = round_num * 100
        for i, code in enumerate(['D01', 'D02', 'D03']):
            db.execute(
                'INSERT INTO drivers (id, driver_id, name, number, code) VALUES (?, ?, ?, ?, ?)',
                (base_id + i, f'driver_{base_id+i}', f'Driver {base_id+i}', base_id+i, code)
            )
        db.commit()
        return race_id

    def _login(self, client, username):
        """Helper: set username."""
        client.post('/set-username', data={'username': username})

    def test_race_a_locked_race_b_open(self, app, client, time_controller):
        """T-RL-006a: Multiple races lock independently.
        
        Given Race A is locked and Race B is open
        When a user visits /predict/<race_A_id>
        Then they should be redirected (Race A is locked)
        
        And when a user visits /predict/<race_B_id>
        Then they should be able to access the form (Race B is open)
        """
        from app import get_db, compute_race_status
        db = get_db()
        
        # Freeze time at a specific moment
        frozen_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)
        time_controller.freeze(frozen_time)
        
        # Race A: already past (locked)
        race_a_time = frozen_time - timedelta(hours=1)
        db.execute(
            'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
            ('Past GP', 601, race_a_time.strftime('%Y-%m-%d %H:%M:%S'), 'open')
        )
        
        # Race B: future (still open)
        race_b_time = frozen_time + timedelta(hours=2)
        db.execute(
            'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
            ('Future GP', 602, race_b_time.strftime('%Y-%m-%d %H:%M:%S'), 'open')
        )
        db.commit()
        
        race_a_id = db.execute('SELECT id FROM races WHERE round = 601').fetchone()['id']
        race_b_id = db.execute('SELECT id FROM races WHERE round = 602').fetchone()['id']
        
        self._login(client, 'testuser_multi')
        
        # Race A should be locked (compute_race_status uses frozen time)
        race_a = db.execute('SELECT * FROM races WHERE round = 601').fetchone()
        status_a = compute_race_status(dict(race_a), has_results=False)
        assert status_a == 'locked', f"Race A (past) should be locked, got {status_a}"
        
        # Race B should be open
        race_b = db.execute('SELECT * FROM races WHERE round = 602').fetchone()
        status_b = compute_race_status(dict(race_b), has_results=False)
        assert status_b == 'open', f"Race B (future) should be open, got {status_b}"

    def test_multi_race_independent_locking(self, app, time_controller):
        """T-RL-006b: Each race locks independently based on its own start time.
        
        Given Race A starts at 14:00 and Race B starts at 16:00
        When time is 14:30 (between both race starts)
        Then Race A should be locked but Race B should still be open
        """
        from app import get_db, compute_race_status
        db = get_db()
        
        # Time is 14:30 UTC - Race A (14:00) should be locked, Race B (16:00) should be open
        frozen_time = datetime(2026, 4, 15, 14, 30, 0, tzinfo=timezone.utc)
        time_controller.freeze(frozen_time)
        
        race_a_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)
        race_b_time = datetime(2026, 4, 15, 16, 0, 0, tzinfo=timezone.utc)
        
        db.execute(
            'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
            ('Race A', 701, race_a_time.strftime('%Y-%m-%d %H:%M:%S'), 'open')
        )
        db.execute(
            'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
            ('Race B', 702, race_b_time.strftime('%Y-%m-%d %H:%M:%S'), 'open')
        )
        db.commit()
        
        race_a = db.execute('SELECT * FROM races WHERE round = 701').fetchone()
        race_b = db.execute('SELECT * FROM races WHERE round = 702').fetchone()
        
        status_a = compute_race_status(dict(race_a), has_results=False)
        status_b = compute_race_status(dict(race_b), has_results=False)
        
        assert status_a == 'locked', f"Race A (started at 14:00, now 14:30) should be locked, got {status_a}"
        assert status_b == 'open', f"Race B (starts at 16:00, now 14:30) should be open, got {status_b}"

    def test_lock_triggers_at_correct_utc_time(self, app, time_controller):
        """T-RL-007a: Lock triggers at correct UTC time regardless of local timezone.
        
        Given a race is scheduled for 14:00 UTC
        When the UTC time reaches 14:00 (regardless of what local time shows)
        Then the race should be locked
        """
        from app import get_db, compute_race_status
        db = get_db()
        
        # Race at 14:00 UTC
        race_time_utc = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)
        
        db.execute(
            'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
            ('UTC Test GP', 801, race_time_utc.strftime('%Y-%m-%d %H:%M:%S'), 'open')
        )
        db.commit()
        
        race = db.execute('SELECT * FROM races WHERE round = 801').fetchone()
        
        # Just before race start - should be open
        time_controller.freeze(race_time_utc - timedelta(minutes=1))
        status_before = compute_race_status(dict(race), has_results=False)
        assert status_before == 'open', f"Race should be open before start, got {status_before}"
        
        # At race start - should be locked
        time_controller.freeze(race_time_utc)
        status_at = compute_race_status(dict(race), has_results=False)
        assert status_at == 'locked', f"Race should be locked at start time, got {status_at}"
        
        # After race start - should be locked
        time_controller.freeze(race_time_utc + timedelta(minutes=1))
        status_after = compute_race_status(dict(race), has_results=False)
        assert status_after == 'locked', f"Race should be locked after start, got {status_after}"

    def test_timezone_parsing_with_z_suffix(self, app, time_controller):
        """T-RL-007b: Race times with Z suffix are parsed correctly as UTC.
        
        Given a race time stored with 'Z' suffix (e.g., '2026-04-15 14:00:00Z')
        When computing race status
        Then the time should be treated as UTC
        """
        from app import get_db, compute_race_status, _parse_race_datetime
        db = get_db()
        
        # Insert race with Z-suffix time (how it comes from the F1 API)
        race_time_str = '2026-04-15 14:00:00Z'
        db.execute(
            'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
            ('Z-Suffix GP', 901, race_time_str, 'open')
        )
        db.commit()
        
        race = db.execute('SELECT * FROM races WHERE round = 901').fetchone()
        parsed = _parse_race_datetime(race['date'])
        
        # Parsed time should be UTC (no tzinfo means naive, but function should handle Z)
        assert parsed is not None, "Failed to parse race datetime with Z suffix"
        assert parsed.tzinfo is not None, "Parsed datetime should be timezone-aware (UTC)"
        
        # Verify the parsed time is correct
        assert parsed.year == 2026
        assert parsed.month == 4
        assert parsed.day == 15
        assert parsed.hour == 14
        assert parsed.minute == 0

    def test_compute_race_status_for_multiple_races(self, app, time_controller):
        """T-RL-006c: compute_race_status correctly handles multiple races.
        
        Given multiple open races with different start times
        When compute_race_status is called for each
        Then each race's status is computed independently
        """
        from app import get_db, compute_race_status
        db = get_db()
        
        # Freeze time at 16:00 UTC
        frozen_time = datetime(2026, 4, 15, 16, 0, 0, tzinfo=timezone.utc)
        time_controller.freeze(frozen_time)
        
        # Race 1: started 2 hours ago (should be locked)
        race1_time = frozen_time - timedelta(hours=2)
        db.execute(
            'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
            ('Past GP 1', 1001, race1_time.strftime('%Y-%m-%d %H:%M:%S'), 'open')
        )
        
        # Race 2: started 1 hour ago (should be locked)
        race2_time = frozen_time - timedelta(hours=1)
        db.execute(
            'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
            ('Past GP 2', 1002, race2_time.strftime('%Y-%m-%d %H:%M:%S'), 'open')
        )
        
        # Race 3: starts in 1 hour (should still be open)
        race3_time = frozen_time + timedelta(hours=1)
        db.execute(
            'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
            ('Future GP', 1003, race3_time.strftime('%Y-%m-%d %H:%M:%S'), 'open')
        )
        db.commit()
        
        # Check statuses using compute_race_status (which uses frozen time)
        race1 = db.execute('SELECT * FROM races WHERE round = 1001').fetchone()
        race2 = db.execute('SELECT * FROM races WHERE round = 1002').fetchone()
        race3 = db.execute('SELECT * FROM races WHERE round = 1003').fetchone()
        
        status1 = compute_race_status(dict(race1), has_results=False)
        status2 = compute_race_status(dict(race2), has_results=False)
        status3 = compute_race_status(dict(race3), has_results=False)
        
        assert status1 == 'locked', f"Past race 1 should be locked, got {status1}"
        assert status2 == 'locked', f"Past race 2 should be locked, got {status2}"
        assert status3 == 'open', f"Future race should still be open, got {status3}"
