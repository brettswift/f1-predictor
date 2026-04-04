"""Pytest configuration and fixtures."""

import pytest
import os
import sys
import responses

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
    """Mock F1 API responses for testing using responses library.
    
    This fixture uses the responses library to mock HTTP requests.
    It provides an easy way to register mock responses for specific endpoints.
    """
    import responses
    
    # Activate the responses mock
    with responses.RequestsMock() as rsps:
        # Default: return empty races list
        rsps.add(
            responses.GET,
            'https://api.jolpi.ca/ergast/f1/2026/1/results.json',
            json={"MRData": {"RaceTable": {"Races": []}}},
            status=200
        )
        yield rsps


@pytest.fixture
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
