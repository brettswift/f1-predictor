"""Pytest configuration and fixtures."""

import pytest
import os
import sys

# Set environment variables BEFORE anything else
os.environ['DATABASE_PATH'] = ':memory:'
os.environ['TESTING'] = 'true'
os.environ['F1_API_URL'] = 'https://api.jolpi.ca/ergast/f1'

# Add project root and src to path (root needed for 'from tests.utils...' imports)
_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, _root)
sys.path.insert(0, os.path.join(_root, 'src'))

# Import after setting env vars
import app as app_module


@pytest.fixture
def app():
    """Create application for testing."""
    # Get fresh app context
    with app_module.app.app_context():
        # Initialize database schema
        app_module.init_db()
        yield app_module.app


@pytest.fixture
def client(app):
    """Create test client."""
    return app.test_client()


@pytest.fixture
def time_controller():
    """Provide time control for tests."""
    from tests.utils.time_control import TimeController
    controller = TimeController()
    yield controller
    controller.unfreeze()


@pytest.fixture
def runner(app):
    """Create CLI test runner."""
    return app.test_cli_runner()


@pytest.fixture
def mock_f1_api(monkeypatch):
    """Mock F1 API responses for testing."""
    class MockResponse:
        def __init__(self, json_data, status_code=200):
            self.json_data = json_data
            self.status_code = status_code
        
        def json(self):
            return self.json_data
    
    def mock_get(*args, **kwargs):
        return MockResponse({"MRData": {"RaceTable": {"Races": []}}})
    
    monkeypatch.setattr("requests.get", mock_get)
    return mock_get


@pytest.fixture(autouse=True)
def clean_env():
    """Ensure clean environment for each test."""
    # Store original values
    orig_db_path = os.environ.get('DATABASE_PATH')
    orig_testing = os.environ.get('TESTING')
    
    # Set test values
    os.environ['DATABASE_PATH'] = ':memory:'
    os.environ['TESTING'] = 'true'
    
    yield
    
    # Restore original values after test
    if orig_db_path is not None:
        os.environ['DATABASE_PATH'] = orig_db_path
    if orig_testing is not None:
        os.environ['TESTING'] = orig_testing
