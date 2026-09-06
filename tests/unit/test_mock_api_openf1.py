"""Unit tests for f1-mock-api OpenF1-shaped endpoints."""

import os
import sys
import tempfile

import pytest

_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(_root, 'f1-mock-api', 'src'))

import app as mock_app


@pytest.fixture
def client():
    """Provide a test client with an isolated temp database."""
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)

    orig_db = mock_app.app.config['DATABASE']
    mock_app.app.config['DATABASE'] = path
    mock_app.app.config['TESTING'] = True

    with mock_app.app.app_context():
        mock_app.init_db()
        mock_app.get_db()
        db = mock_app.get_db()
        db.execute("INSERT OR IGNORE INTO seasons (season) VALUES ('2024')")
        db.execute('''
            INSERT INTO races (
                season, round, race_name, circuit_name, country, locality,
                date, time, has_results, p1_driver_id, p2_driver_id, p3_driver_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', ('2024', '1', 'Bahrain Grand Prix', 'Bahrain International Circuit',
              'Bahrain', 'Sakhir', '2024-03-02', '15:00:00Z', 1,
              'verstappen', 'perez', 'leclerc'))
        for driver_id, number, code, given, family, nationality in [
            ('verstappen', '1', 'VER', 'Max', 'Verstappen', 'Dutch'),
            ('perez', '11', 'PER', 'Sergio', 'Perez', 'Mexican'),
            ('leclerc', '16', 'LEC', 'Charles', 'Leclerc', 'Monegasque'),
        ]:
            db.execute('''
                INSERT INTO drivers (
                    season, driver_id, permanent_number, code,
                    given_name, family_name, nationality
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', ('2024', driver_id, number, code, given, family, nationality))
        db.commit()

    with mock_app.app.test_client() as test_client:
        yield test_client

    mock_app.app.config['DATABASE'] = orig_db
    if os.path.exists(path):
        os.unlink(path)


class TestOpenF1Routes:
    def test_health(self, client):
        resp = client.get('/health')
        assert resp.status_code == 200
        assert resp.get_json()['status'] == 'ok'

    def test_sessions(self, client):
        resp = client.get('/v1/sessions?year=2024&session_type=Race')
        assert resp.status_code == 200
        data = resp.get_json()
        assert len(data) == 1
        assert data[0]['session_key'] == 1
        assert data[0]['session_type'] == 'Race'
        assert data[0]['circuit_short_name'] == 'Bahrain International Circuit'

    def test_meetings(self, client):
        resp = client.get('/v1/meetings?year=2024')
        assert resp.status_code == 200
        data = resp.get_json()
        assert len(data) == 1
        assert data[0]['meeting_key'] == 1
        assert data[0]['meeting_name'] == 'Bahrain Grand Prix'

    def test_drivers(self, client):
        resp = client.get('/v1/drivers?session_key=1')
        assert resp.status_code == 200
        data = resp.get_json()
        assert len(data) == 3
        numbers = {d['driver_number'] for d in data}
        assert numbers == {1, 11, 16}

    def test_session_result(self, client):
        resp = client.get('/v1/session_result?session_key=1')
        assert resp.status_code == 200
        data = resp.get_json()
        assert len(data) == 3
        assert data[0]['driver_number'] == 1
        assert data[0]['position'] == 1
        assert data[1]['driver_number'] == 11
        assert data[1]['position'] == 2
        assert data[2]['driver_number'] == 16
        assert data[2]['position'] == 3

    def test_session_result_not_finished(self, client):
        db = mock_app.get_db()
        db.execute("UPDATE races SET has_results = 0 WHERE season = '2024' AND round = '1'")
        db.commit()
        resp = client.get('/v1/session_result?session_key=1')
        assert resp.status_code == 200
        assert resp.get_json() == []

    def test_starting_grid_and_race_control(self, client):
        assert client.get('/v1/starting_grid?session_key=1').get_json() == []
        assert client.get('/v1/race_control?session_key=1').get_json() == []


class TestLegacyErgastRoutes:
    def test_season_races(self, client):
        resp = client.get('/2024.json')
        assert resp.status_code == 200
        data = resp.get_json()
        races = data['MRData']['RaceTable']['Races']
        assert len(races) == 1
        assert races[0]['raceName'] == 'Bahrain Grand Prix'

    def test_season_drivers(self, client):
        resp = client.get('/2024/drivers.json')
        assert resp.status_code == 200
        data = resp.get_json()
        drivers = data['MRData']['DriverTable']['Drivers']
        assert len(drivers) == 3

    def test_race_results(self, client):
        resp = client.get('/2024/1/results.json')
        assert resp.status_code == 200
        data = resp.get_json()
        results = data['MRData']['RaceTable']['Races'][0]['Results']
        assert len(results) == 3
        assert results[0]['position'] == '1'
