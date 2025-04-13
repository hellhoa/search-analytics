# Epsilo Take-home Assignment: Search Analytics API

## System Design

### Database Schema
- **keyword**: Stores keywords and their IDs
- **keyword_search_volume**: Stores hourly search volume data
- **user**: Stores user information
- **subscription_type**: Defines subscription types (hourly/daily)
- **user_subscription**: Maps users to their keyword subscriptions
- **daily_keyword_search_volume**: Stores aggregated daily search volumes

### Components
1. **FastAPI Application**
   - Handles API requests
   - Validates user subscriptions
   - Returns search volume data based on subscription type

2. **Airflow DAG**
   - Runs every hour (at minute 1)
   - Aggregates hourly data to daily snapshots
   - Selects data points closest to 9:00 AM for daily aggregation

3. **Data Generator**
   - Populates test data
   - Creates sample users and subscriptions

## Setup Instructions

### Prerequisites
- Docker
- Docker Compose
- curl (for testing)

### Environment Setup

1. Clone the repository:
```bash
git clone <repository-url>
```

2. Go to the project folder:
```bash
cd epsilo-tha
```

### Running the Application

1. Start all services:
```bash
docker-compose up -d
```

2. Wait for services to initialize (about 30 seconds)

3. Verify services are running:
```bash
docker-compose ps
```
4. Access to the Airflow UI at http://localhost:8080 (use admin/admin for username and password) and run the `daily_search_volume_aggregation` DAG.

### Testing the API

1. Test the health check endpoint:
```bash
curl http://localhost:5000/
```

2. Query search volume data:
```bash
curl -X POST http://localhost:5000/api/search-volume \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1,
    "keywords": ["python programming"],
    "timing": "hourly",
    "start_time": "2025-01-01 00:00:00",
    "end_time": "2025-01-02 00:00:00"
  }'
```

### Monitoring

1. Access Airflow UI:
   - URL: http://localhost:8080
   - Username: admin
   - Password: admin

2. Check DAG status:
   - Look for "daily_search_volume_aggregation" DAG
   - Verify hourly runs
   - Check task logs for execution details

## Project Structure

```
epsilo-tha/
├── app/
│   ├── app.py              # FastAPI application
│   └── data_generator.py   # Test data generator
├── airflow/
│   └── dags/
│       ├── daily_search_volume_aggregation.py  # Airflow DAG
│       └── spark/
│           ├── daily_aggregation.py            # Spark job
│           └── jars/                           # Java dependencies
├── mysql-init/
│   └── init.sql           # Database initialization
├── docker-compose.yml     # Service orchestration
└── Dockerfile            # Airflow container configuration
```

## Design Decisions

1. **Data Storage**
   - Separate tables for hourly and daily data
   - Composite primary keys for efficient querying
   - Foreign key constraints for data integrity

2. **Data Processing**
   - Spark for efficient data aggregation
   - Window functions for selecting representative daily data
   - Airflow for reliable scheduling

3. **API Design**
   - Subscription-based access control
   - Flexible timing options (hourly/daily)
   - JSON response format for easy integration

## Testing

### Running Tests

1. Install test dependencies:
```bash
python3 -m pip install pytest pytest-cov pytest-asyncio coverage
```

2. Run tests:
```bash
python3 -m pytest app/tests/test_app.py -v
```

3. Run tests with coverage report:
```bash
python3 -m coverage run -m pytest app/tests/test_app.py && python3  -m coverage report -m
```

### Docker Test Environment

To run tests in the Docker environment:

```bash
# Build and start services
docker-compose up -d

# Install test dependencies in the API container
docker-compose exec api pip install pytest pytest-cov pytest-asyncio

# Run tests in the API container
docker-compose exec api python3 -m pytest /app/tests/test_app.py -v

# View coverage report
docker-compose exec api python3 -m coverage run -m pytest app/tests/test_app.py && python3  -m coverage report -m
```

### Test Cases

The test suite includes various scenarios:

1. **Basic Functionality**
   - Health check endpoint (`test_root_endpoint`)
   - Valid hourly data requests (`test_valid_hourly_request`)
   - Valid daily data requests (`test_valid_daily_request`)

2. **Subscription Validation**
   - Invalid subscription type (`test_invalid_subscription_type`)
   - Expired subscriptions (`test_expired_subscription`)
   - Outside subscription period (`test_outside_subscription_period`)
   - Overlapping subscription periods
     - Union time ranges (`test_union_time_ranges`)
     - Non-mutual time ranges (`test_non_mutual_time_ranges`)

3. **Input Validation**
   - Invalid timing values (`test_invalid_timing`)
   - Invalid date format (`test_invalid_date_format`)
   - End date before start date (`test_end_date_before_start_date`)
   - Empty keywords list (`test_empty_keywords_list`)
   - Nonexistent keywords (`test_nonexistent_keyword`)
   - Future date ranges (`test_future_date_range`)

4. **Multiple Keywords**
   - Multiple valid keywords (`test_multiple_keywords`)
   - Mix of subscribed and unsubscribed keywords (`test_mixed_subscription_keywords`)

### Docker Test Environment

To run tests in the Docker environment:

```bash
# Build and start services
docker-compose up -d

# Run tests in the API container
docker-compose exec api pytest /app/tests/test_app.py -v --cov=app

# View coverage report
docker-compose exec api pytest /app/tests/test_app.py --cov=app --cov-report=term-missing
```

## Troubleshooting

1. If services fail to start:
```bash
docker-compose down -v
docker-compose up -d
```

2. To view service logs:
```bash
docker-compose logs -f <service-name>
```

3. To reset the database:
```bash
docker-compose down -v
docker-compose up -d
```

## Implementation Challenge and Future Improvements

### Performance Considerations

Initially, the daily data aggregation was planned to be implemented using a MySQL view:

```sql
CREATE VIEW daily_keyword_search_volume AS
WITH ranked_data AS (
    SELECT 
        keyword_id,
        DATE(created_datetime) AS date,
        created_datetime,
        search_volume,
        RANK() OVER (
            PARTITION BY keyword_id, DATE(created_datetime) 
            ORDER BY ABS(HOUR(created_datetime) - 9)
        ) AS rank_by_time_diff
    FROM keyword_search_volume
)
SELECT keyword_id, date, created_datetime, search_volume
FROM ranked_data
WHERE rank_by_time_diff = 1;
```

However, this approach presented several limitations:
- Poor performance with large-scale data
- High computational overhead for complex window functions
- Increased load on the production database
- Limited scalability for growing data volumes

### Current Solution

The implementation was improved by:
1. Using Apache Spark for data aggregation
   - Efficient distributed processing
   - Better handling of large-scale data
   - Reduced load on production database

2. Implementing Airflow DAG
   - Scheduled to run at minute 1 of every hour
   - Ensures data completeness before aggregation
   - Reliable scheduling and monitoring
   - Built-in retry mechanisms

### Future Improvements

1. **Real-time Processing**
   - Implement Apache Flink or Spark Structured Streaming
   - Reduce data latency
   - Enable near real-time aggregations
   - Improve data freshness

2. **Performance Optimizations**
   - Add data partitioning strategies
   - Implement caching mechanisms
   - Optimize Spark configurations
   - Add incremental processing

3. **Monitoring and Alerts**
   - Add data quality checks
   - Implement SLA monitoring
   - Set up alerting for job failures
   - Add performance metrics tracking