from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, RootModel
from typing import List, Optional
import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool
from datetime import datetime, timezone
import os
from contextlib import contextmanager, asynccontextmanager


# Database connection pool
connection_pool = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle event handler for the FastAPI application"""
    # Initialize connection pool on startup
    init_db_pool()
    yield
    # Cleanup can go here if needed
    
# Initialize FastAPI app with lifespan handler
app = FastAPI(
    title="Search Analytics API",
    description="API for querying search volume data based on user subscriptions",
    version="1.0.0",
    lifespan=lifespan
)

def init_db_pool():
    global connection_pool
    if connection_pool is None:
        # Get environment variables
        host = os.getenv("MYSQL_HOST", "localhost")
        user = os.getenv("MYSQL_USER", "root")
        password = os.getenv("MYSQL_PASSWORD", "password")
        database = os.getenv("MYSQL_DATABASE", "search_analytics")
        
        # Create connection pool
        connection_pool = MySQLConnectionPool(
            pool_name="search_analytics_pool",
            pool_size=5,
            host=host,
            user=user,
            password=password,
            database=database
        )
    return connection_pool

@contextmanager
def get_db_connection():
    """Get a database connection from the pool"""
    pool = init_db_pool()
    conn = pool.get_connection()
    try:
        yield conn
    finally:
        conn.close()

# Pydantic models for request/response validation
class SearchVolumeRequestData(BaseModel):
    user_id: int
    keywords: List[str]
    timing: str = Field(..., description="Either 'hourly' or 'daily'")
    start_time: str = Field(..., description="Format: YYYY-MM-DD HH:MM:SS")
    end_time: str = Field(..., description="Format: YYYY-MM-DD HH:MM:SS")

class SearchVolumeRequest(RootModel):
    root: SearchVolumeRequestData

class DataPoint(BaseModel):
    datetime: str
    search_volume: int

class SearchVolumeResponse(RootModel):
    root: dict[str, List[DataPoint]]



@app.get("/", status_code=status.HTTP_200_OK)
async def root():
    """Health check endpoint"""
    return {"status": "ok", "message": "Search Analytics API is running"}

@app.post("/api/search-volume", response_model=SearchVolumeResponse, status_code=status.HTTP_200_OK)
async def get_search_volume(request: SearchVolumeRequest):
    """
    Get search volume data for specified keywords based on user subscription
    """
    # Access the data through the root field
    request_data = request.root
    user_id = request_data.user_id
    keywords = request_data.keywords
    timing = request_data.timing
    start_time = request_data.start_time
    end_time = request_data.end_time

    # Validate non-empty keywords list
    if not keywords or len(keywords) == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Keywords list cannot be empty"
        )
    
    # Validate date format and range
    try:
        start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')

        # Make naive datetimes timezone-aware
        start_time = start_time.replace()
        end_time = end_time.replace()

        # Check if dates are in the future
        current_time = datetime.now()
        if start_time > current_time or end_time > current_time:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot query future dates"
            )
        
        # Check if end_time is before start_time
        if end_time < start_time:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="End time must be after start time"
            )
        
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid date format. Use YYYY-MM-DD HH:MM:SS"
        )
    
    # Validate timing
    if timing not in ['hourly', 'daily']:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Timing must be either 'hourly' or 'daily'"
        )
    
    # Convert timing to subscription_type_id
    timing_id = 1 if timing == 'hourly' else 2
    
    # Get user subscriptions
    with get_db_connection() as conn:
        cursor = conn.cursor(dictionary=True)
        try:
            # Get subscriptions for user and keywords
            subscription_query = """
                SELECT k.keyword_id, k.keyword_name, us.subscription_type_id, 
                    us.start_date, us.end_date
                FROM keyword k
                JOIN user_subscription us ON k.keyword_id = us.keyword_id
                WHERE us.user_id = %s AND k.keyword_name IN ({})
                ORDER BY us.start_date
            """.format(','.join(['%s'] * len(keywords)))
            
            cursor.execute(subscription_query, [user_id] + keywords)
            subscriptions = cursor.fetchall()
            
            # Group subscriptions by keyword and validate existence
            keyword_subscriptions = {}
            for sub in subscriptions:
                if sub['keyword_name'] not in keyword_subscriptions:
                    keyword_subscriptions[sub['keyword_name']] = []
                keyword_subscriptions[sub['keyword_name']].append(sub)
            
            # Check for missing keywords
            missing_keywords = set(keywords) - set(keyword_subscriptions.keys())
            if missing_keywords:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Keywords not found: {missing_keywords}"
                )
            
            # Process data for each keyword
            result = {}
            for keyword, subs in keyword_subscriptions.items():
                # Check subscription type compatibility
                valid_subscription = any(
                    sub['subscription_type_id'] <= timing_id  # hourly(1) can access daily(2)
                    for sub in subs
                )
                if not valid_subscription:
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=f"Invalid subscription type for {keyword}"
                    )
                
                # Collect all subscription periods
                subscription_periods = [
                    (sub['start_date'], sub['end_date'] or datetime.max)
                    for sub in subs
                ]
                
                # Merge overlapping periods
                merged_periods = merge_time_ranges(subscription_periods)
                
                # Check if requested period is covered by any merged period
                request_covered = False
                for period_start, period_end in merged_periods:
                    if period_start <= start_time and period_end >= end_time:
                        request_covered = True
                        break
                
                if not request_covered:
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=f"No valid subscription for {keyword} during requested period"
                    )
                
                # Get search volume data
                if timing == 'hourly':
                    query = """
                    SELECT created_datetime, search_volume
                    FROM keyword_search_volume
                    WHERE keyword_id = %s
                    AND created_datetime BETWEEN %s AND %s
                    ORDER BY created_datetime
                    """
                else:  # daily
                    query = """
                    SELECT date as created_datetime, search_volume
                    FROM daily_keyword_search_volume
                    WHERE keyword_id = %s
                    AND date BETWEEN DATE(%s) AND DATE(%s)
                    ORDER BY date
                    """
                
                cursor.execute(query, (subs[0]['keyword_id'], start_time, end_time))
                data = cursor.fetchall()
                
                # Format response data
                result[keyword] = [
                    {
                        "datetime": row["created_datetime"].strftime('%Y-%m-%d %H:%M:%S'),
                        "search_volume": row["search_volume"]
                    } for row in data
                ]
            
        finally:
            cursor.close()
    
    return result

def merge_time_ranges(periods):
    """Merge overlapping time periods and return union of ranges"""
    if not periods:
        return []
    
    # Sort periods by start time
    sorted_periods = sorted(periods, key=lambda x: x[0])
    merged = [sorted_periods[0]]
    
    for current in sorted_periods[1:]:
        previous = merged[-1]
        # If current period overlaps with previous, merge them
        if current[0] <= previous[1]:
            merged[-1] = (previous[0], max(previous[1], current[1]))
        else:
            merged.append(current)
    
    return merged