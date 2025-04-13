from fastapi.testclient import TestClient
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
import sys
from httpx import AsyncClient
from pathlib import Path
import asyncio

# Add parent directory to path to import app
sys.path.append(str(Path(__file__).parent.parent))
from app import app, get_db_connection

client = TestClient(app)

@pytest.fixture
async def async_client():
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client


@pytest.fixture
def mock_db_conn():
    """Mock database connection"""
    with patch('app.get_db_connection') as mock_context:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_context.return_value.__enter__.return_value = mock_conn
        mock_context.return_value.__exit__.return_value = None
        yield mock_conn, mock_cursor

def test_root():
    """Test root health check endpoint"""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "message": "Search Analytics API is running"}

def test_valid_hourly_request(mock_db_conn):
    """Test valid request with hourly data for hourly subscription"""
    _, mock_cursor = mock_db_conn
    
    # Mock subscription query results
    mock_cursor.fetchall.side_effect = [
        # Subscription query result
        [{'keyword_id': 1, 'subscription_type_id': 1, 'start_date': datetime(2025, 1, 1), 
          'end_date': datetime(2025, 4, 1), 'keyword_name': 'python programming'}],
        # Search volume data
        [
            {'created_datetime': datetime(2025, 2, 1, 9, 0, 0), 'search_volume': 5000},
            {'created_datetime': datetime(2025, 2, 1, 10, 0, 0), 'search_volume': 5200},
            {'created_datetime': datetime(2025, 2, 1, 11, 0, 0), 'search_volume': 5500}
        ]
    ]
    
    request_data = {
        'user_id': 1,
        'keywords': ['python programming'],
        'timing': 'hourly',
        'start_time': '2025-02-01 09:00:00',
        'end_time': '2025-02-01 11:00:00'
    }
    
    response = client.post("/api/search-volume", json=request_data)
    assert response.status_code == 200
    data = response.json()
    assert 'python programming' in data
    assert len(data['python programming']) == 3

def test_daily_subscription_requesting_hourly(mock_db_conn):
    """Test request for hourly data with only daily subscription"""
    _, mock_cursor = mock_db_conn
    
    mock_cursor.fetchall.side_effect = [
        # Subscription query result - daily subscription
        [{'keyword_id': 2, 'subscription_type_id': 2, 'start_date': datetime(2025, 1, 1), 
          'end_date': datetime(2025, 4, 1), 'keyword_name': 'data science'}]
    ]
    
    request_data = {
        'user_id': 2,
        'keywords': ['data science'],
        'timing': 'hourly',
        'start_time': '2025-02-01 00:00:00',
        'end_time': '2025-02-02 00:00:00'
    }
    
    response = client.post("/api/search-volume", json=request_data)
    assert response.status_code == 403

def test_overlapping_subscriptions(mock_db_conn):
    """Test request with overlapping subscription periods"""
    _, mock_cursor = mock_db_conn
    
    mock_cursor.fetchall.side_effect = [
        # Multiple subscription periods
        [
            {
                'keyword_id': 1,
                'subscription_type_id': 1,
                'start_date': datetime(2025, 1, 1),
                'end_date': datetime(2025, 2, 15),
                'keyword_name': 'python programming'
            },
            {
                'keyword_id': 1,
                'subscription_type_id': 1,
                'start_date': datetime(2025, 2, 1),
                'end_date': datetime(2025, 3, 15),
                'keyword_name': 'python programming'
            }
        ],
        # Search volume data
        [{'created_datetime': datetime(2025, 2, 10, 9, 0, 0), 'search_volume': 5000}]
    ]
    
    request_data = {
        'user_id': 1,
        'keywords': ['python programming'],
        'timing': 'hourly',
        'start_time': '2025-02-10 09:00:00',
        'end_time': '2025-02-10 09:00:00'
    }
    
    response = client.post("/api/search-volume", json=request_data)
    assert response.status_code == 200
    data = response.json()
    assert 'python programming' in data
    assert len(data['python programming']) == 1

def test_hourly_subscription_requesting_daily(mock_db_conn):
    """Test request for daily data with hourly subscription"""
    _, mock_cursor = mock_db_conn
    
    mock_cursor.fetchall.side_effect = [
        # Subscription query result - hourly subscription
        [{'keyword_id': 1, 'subscription_type_id': 1, 'start_date': datetime(2025, 1, 1), 
          'end_date': datetime(2025, 4, 1), 'keyword_name': 'python programming'}],
        # Daily search volume data
        [
            {'created_datetime': datetime(2025, 2, 1, 9, 0, 0), 'search_volume': 5000},
            {'created_datetime': datetime(2025, 2, 2, 9, 0, 0), 'search_volume': 5200}
        ]
    ]
    
    request_data = {
        'user_id': 1,
        'keywords': ['python programming'],
        'timing': 'daily',
        'start_time': '2025-02-01 00:00:00',
        'end_time': '2025-02-02 23:59:59'
    }
    
    response = client.post("/api/search-volume", json=request_data)
    assert response.status_code == 200
    data = response.json()
    assert 'python programming' in data
    assert len(data['python programming']) == 2

def test_unsubscribed_keyword(mock_db_conn):
    """Test request for keyword user is not subscribed to"""
    _, mock_cursor = mock_db_conn
    
    # Mock empty subscription result
    mock_cursor.fetchall.return_value = []
    
    request_data = {
        'user_id': 1,
        'keywords': ['blockchain technology'],
        'timing': 'hourly',
        'start_time': '2025-02-01 00:00:00',
        'end_time': '2025-02-02 00:00:00'
    }
    
    response = client.post("/api/search-volume", json=request_data)
    assert response.status_code == 404

def test_outside_subscription_period(mock_db_conn):
    """Test request outside subscription period"""
    _, mock_cursor = mock_db_conn
    
    mock_cursor.fetchall.side_effect = [
        # Subscription query result
        [{'keyword_id': 1, 'subscription_type_id': 1, 'start_date': datetime(2025, 2, 15), 
          'end_date': datetime(2025, 3, 15), 'keyword_name': 'python programming'}]
    ]
    
    request_data = {
        'user_id': 1,
        'keywords': ['python programming'],
        'timing': 'hourly',
        'start_time': '2025-01-01 00:00:00',
        'end_time': '2025-01-10 00:00:00'
    }
    
    response = client.post("/api/search-volume", json=request_data)
    assert response.status_code == 403
    assert 'detail' in response.json()

async def test_multiple_keywords(mock_db_conn, async_client):
    """Test valid request with multiple keywords"""
    _, mock_cursor = mock_db_conn
    
    mock_cursor.fetchall.side_effect = [
        # Subscription query results for both keywords
        [
            {
                'keyword_id': 1, 
                'subscription_type_id': 1, 
                'start_date': datetime(2025, 1, 1), 
                'end_date': datetime(2025, 4, 1), 
                'keyword_name': 'python programming'
            },
            {
                'keyword_id': 2, 
                'subscription_type_id': 1, 
                'start_date': datetime(2025, 1, 1), 
                'end_date': datetime(2025, 4, 1), 
                'keyword_name': 'data science'
            }
        ],
        # Search volume data for both keywords
        [{'created_datetime': datetime(2025, 2, 1, 9, 0, 0), 'search_volume': 5000}],
        [{'created_datetime': datetime(2025, 2, 1, 9, 0, 0), 'search_volume': 6000}]
    ]
    
    request_data = {
        'user_id': 1,
        'keywords': ['python programming', 'data science'],
        'timing': 'hourly',
        'start_time': '2025-02-01 09:00:00',
        'end_time': '2025-02-01 09:00:00'
    }
    
    response = await async_client.post("/api/search-volume", json=request_data)
    assert response.status_code == 200
    
    data = response.json()
    assert len(data) == 2
    assert 'python programming' in data
    assert 'data science' in data
    assert len(data['python programming']) == 1
    assert len(data['data science']) == 1
    assert data['python programming'][0]['search_volume'] == 5000
    assert data['data science'][0]['search_volume'] == 6000

def test_invalid_timing():
    """Test invalid timing value"""
    request_data = {
        'user_id': 1,
        'keywords': ['python programming'],
        'timing': 'weekly',  # Invalid timing
        'start_time': '2025-02-01 00:00:00',
        'end_time': '2025-02-02 00:00:00'
    }
    
    response = client.post("/api/search-volume", json=request_data)
    assert response.status_code == 400

def test_invalid_date_format():
    """Test invalid date format"""
    request_data = {
        'user_id': 1,
        'keywords': ['python programming'],
        'timing': 'hourly',
        'start_time': '2025-02-01',  # Invalid format, missing time
        'end_time': '2025-02-02 00:00:00'
    }
    
    response = client.post("/api/search-volume", json=request_data)
    assert response.status_code == 400

def test_end_date_before_start_date():
    """Test when end_date is before start_date"""
    request_data = {
        'user_id': 1,
        'keywords': ['python programming'],
        'timing': 'hourly',
        'start_time': '2025-02-02 00:00:00',
        'end_time': '2025-02-01 00:00:00'  # Before start_time
    }
    
    response = client.post("/api/search-volume", json=request_data)
    assert response.status_code == 400

def test_empty_keywords_list():
    """Test request with empty keywords list"""
    request_data = {
        'user_id': 1,
        'keywords': [],
        'timing': 'hourly',
        'start_time': '2025-02-01 00:00:00',
        'end_time': '2025-02-02 00:00:00'
    }
    
    response = client.post("/api/search-volume", json=request_data)
    assert response.status_code == 400

def test_nonexistent_keyword(mock_db_conn):
    """Test request with keyword that doesn't exist in database"""
    _, mock_cursor = mock_db_conn
    
    # Mock empty result for keyword lookup
    mock_cursor.fetchall.return_value = []
    
    request_data = {
        'user_id': 1,
        'keywords': ['nonexistent keyword'],
        'timing': 'hourly',
        'start_time': '2025-02-01 00:00:00',
        'end_time': '2025-02-02 00:00:00'
    }
    
    response = client.post("/api/search-volume", json=request_data)
    assert response.status_code == 404

def test_expired_subscription(mock_db_conn):
    """Test request with expired subscription"""
    _, mock_cursor = mock_db_conn
    
    mock_cursor.fetchall.side_effect = [
        # Subscription query result with past end_date
        [{'keyword_id': 1, 'subscription_type_id': 1, 'start_date': datetime(2024, 1, 1), 
          'end_date': datetime(2024, 12, 31), 'keyword_name': 'python programming'}]
    ]
    
    request_data = {
        'user_id': 1,
        'keywords': ['python programming'],
        'timing': 'hourly',
        'start_time': '2025-02-01 00:00:00',
        'end_time': '2025-02-02 00:00:00'
    }
    
    response = client.post("/api/search-volume", json=request_data)
    assert response.status_code == 403

def test_future_date_range():
    """Test request with future date range"""
    request_data = {
        'user_id': 1,
        'keywords': ['python programming'],
        'timing': 'hourly',
        'start_time': '2026-01-01 00:00:00',  # Future date
        'end_time': '2026-01-02 00:00:00'
    }
    
    response = client.post("/api/search-volume", json=request_data)
    assert response.status_code == 400

def test_union_time_ranges(mock_db_conn):
    """Test request with overlapping subscription periods that form a union"""
    _, mock_cursor = mock_db_conn
    
    mock_cursor.fetchall.side_effect = [
        # Subscription periods that overlap
        [
            {
                'keyword_id': 1,
                'subscription_type_id': 1,
                'start_date': datetime(2025, 1, 1),
                'end_date': datetime(2025, 3, 15),
                'keyword_name': 'python programming'
            },
            {
                'keyword_id': 1,
                'subscription_type_id': 1,
                'start_date': datetime(2025, 2, 1),
                'end_date': datetime(2025, 4, 10),
                'keyword_name': 'python programming'
            }
        ],
        # Search volume data within the union period
        [{'created_datetime': datetime(2025, 4, 1, 9, 0, 0), 'search_volume': 5000}]
    ]
    
    request_data = {
        'user_id': 1,
        'keywords': ['python programming'],
        'timing': 'hourly',
        'start_time': '2025-04-01 09:00:00',  # After first subscription ends but within second
        'end_time': '2025-04-01 09:00:00'
    }
    
    response = client.post("/api/search-volume", json=request_data)
    assert response.status_code == 200
    data = response.json()
    assert 'python programming' in data
    assert len(data['python programming']) == 1
    assert data['python programming'][0]['search_volume'] == 5000

def test_non_mutual_time_ranges(mock_db_conn):
    """Test request with non-overlapping subscription periods"""
    _, mock_cursor = mock_db_conn
    
    mock_cursor.fetchall.side_effect = [
        # Non-overlapping subscription periods
        [
            {
                'keyword_id': 1,
                'subscription_type_id': 1,
                'start_date': datetime(2025, 1, 1),
                'end_date': datetime(2025, 2, 28),
                'keyword_name': 'python programming'
            },
            {
                'keyword_id': 1,
                'subscription_type_id': 1,
                'start_date': datetime(2025, 4, 1),
                'end_date': datetime(2025, 4, 11),
                'keyword_name': 'python programming'
            }
        ]
    ]
    
    # Test request in the gap between subscriptions
    request_data = {
        'user_id': 1,
        'keywords': ['python programming'],
        'timing': 'hourly',
        'start_time': '2025-03-15 09:00:00',  # Between subscription periods
        'end_time': '2025-03-15 09:00:00'
    }
    
    response = client.post("/api/search-volume", json=request_data)
    assert response.status_code == 403
    assert 'No valid subscription' in response.json()['detail']