"""Example integration test for health endpoint."""
import pytest


@pytest.mark.integration
def test_health_endpoint(client):
    """Test health endpoint returns 200."""
    response = client.get('/health')
    assert response.status_code == 200
    data = response.get_json()
    assert data['status'] == 'healthy'
