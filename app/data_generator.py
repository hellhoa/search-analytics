import mysql.connector
import random
from datetime import datetime, timedelta
import pandas as pd
import os
import time

# Database connection
def get_db_connection():
    # Get environment variables
    host = os.getenv("MYSQL_HOST", "localhost")
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "password")
    database = os.getenv("MYSQL_DATABASE", "search_analytics")
    
    # Try to connect with retries
    max_retries = 10
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            return mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
        except mysql.connector.Error as err:
            print(f"Database connection attempt {attempt+1} failed: {err}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise

# Generate keywords
def generate_keywords():
    keywords = [
        "python programming",
        "data science",
        "machine learning",
        "artificial intelligence",
        "web development",
        "cloud computing",
        "database management",
        "blockchain technology",
        "cybersecurity basics",
        "mobile app development",
        "devops practices",
        "software testing",
        "javascript frameworks",
        "api development",
        "docker containers",
        "kubernetes orchestration",
        "data visualization",
        "natural language processing",
        "big data analytics",
        "cloud security",
        "cybersecurity",
        "game development",
        "ux/ui design",
        "product management",
        "project management",
        "project management tools",
        "agile methodologies",
        "software development methodologies",
        "version control systems",
        "source code management",
        "continuous integration",
        "continuous deployment",
        "software development process",
        "software development methodologies",
        "software development tools",
    ]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for i, keyword in enumerate(keywords, 1):
        cursor.execute(
            "INSERT INTO keyword (keyword_id, keyword_name) VALUES (%s, %s)",
            (i, keyword)
        )
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return {i: keyword for i, keyword in enumerate(keywords, 1)}

# Generate search volume data
def generate_search_volumes(keywords):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 3 months of hourly data
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 4, 10)
    
    # Prepare list to collect all data
    all_data = []
    
    current_date = start_date
    while current_date < end_date:
        for keyword_id in keywords.keys():
            # Base volume with some randomization
            base_volume = random.randint(1000, 10000)
            
            # Add time-of-day pattern (higher during work hours)
            hour = current_date.hour
            if 9 <= hour <= 17:  # Work hours
                time_factor = 1.5
            elif 6 <= hour < 9 or 17 < hour <= 22:  # Morning/evening
                time_factor = 1.2
            else:  # Night time
                time_factor = 0.7
                
            # Add day-of-week pattern (lower on weekends)
            day_of_week = current_date.weekday()
            day_factor = 0.8 if day_of_week >= 5 else 1.1  # Weekend vs weekday
            
            # Add random trend factor
            trend_factor = 1 + (current_date - start_date).days / 90 * 0.2  # 20% growth over 3 months
            
            # Add some randomness
            random_factor = random.uniform(0.9, 1.1)
            
            # Calculate final volume
            volume = int(base_volume * time_factor * day_factor * trend_factor * random_factor)
            
            # Format datetime to hourly format (yyyy-MM-dd HH:00:00)
            formatted_datetime = current_date.replace(minute=0, second=0)
            
            all_data.append((keyword_id, formatted_datetime, volume))
        
        current_date += timedelta(hours=1)
    
    # Batch insert for better performance (in smaller chunks to avoid memory issues)
    batch_size = 10000
    for i in range(0, len(all_data), batch_size):
        batch = all_data[i:i+batch_size]
        cursor.executemany(
            "INSERT INTO keyword_search_volume (keyword_id, created_datetime, search_volume) VALUES (%s, %s, %s)",
            batch
        )
        conn.commit()
        print(f"Inserted batch {i//batch_size + 1} ({len(batch)} records)")
    
    cursor.close()
    conn.close()
    
    print(f"Generated {len(all_data)} data points for {len(keywords)} keywords")

# Generate sample users
def generate_users():
    users = [
        (1, "alice", "alice@example.com"),
        (2, "bob", "bob@example.com"),
        (3, "charlie", "charlie@example.com"),
        (4, "diana", "diana@example.com"),
        (5, "evan", "evan@example.com")
    ]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.executemany(
        "INSERT INTO user (user_id, username, email) VALUES (%s, %s, %s)",
        users
    )
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return users

# Generate subscriptions
def generate_subscriptions():
    subscriptions = [
        # User 1 - overlapping hourly subscriptions for python programming
        (1, 1, 1, '2025-01-01 00:00:00', '2025-04-01 00:00:00'),
        (1, 1, 1, '2025-03-01 00:00:00', '2025-06-01 00:00:00'),  # Overlapping period
        
        # Regular subscriptions
        (1, 3, 1, '2025-01-15 00:00:00', '2025-03-15 00:00:00'),
        (2, 2, 2, '2025-01-01 00:00:00', '2025-04-01 00:00:00'),
        (2, 4, 2, '2025-02-01 00:00:00', '2025-03-01 00:00:00'),
        
        # User 3 - overlapping daily subscriptions for web development
        (3, 5, 2, '2025-01-01 00:00:00', '2025-03-15 00:00:00'),
        (3, 5, 2, '2025-02-15 00:00:00', '2025-04-01 00:00:00'),  # Overlapping period
        
        # More regular subscriptions
        (3, 6, 2, '2025-02-15 00:00:00', '2025-04-01 00:00:00'),
        (4, 7, 1, '2025-01-01 00:00:00', '2025-03-15 00:00:00'),
        (4, 8, 2, '2025-01-15 00:00:00', '2025-03-15 00:00:00'),
        
        # User 5 - ongoing subscriptions
        (5, 9, 1, '2025-01-01 00:00:00', None),
        (5, 10, 2, '2025-02-01 00:00:00', None),
        
        # Additional subscriptions for new keywords
        (1, 11, 1, '2025-01-01 00:00:00', '2025-06-01 00:00:00'),
        (2, 12, 2, '2025-01-01 00:00:00', '2025-12-31 00:00:00'),
        (3, 13, 1, '2025-02-01 00:00:00', '2025-05-01 00:00:00'),
        (4, 14, 2, '2025-03-01 00:00:00', '2025-06-01 00:00:00'),
        (5, 15, 1, '2025-01-01 00:00:00', None),
        
        # Overlapping subscriptions for different users
        (1, 16, 1, '2025-01-01 00:00:00', '2025-06-01 00:00:00'),
        (2, 16, 2, '2025-03-01 00:00:00', '2025-12-31 00:00:00'),
        
        # More subscriptions for remaining keywords
        (3, 17, 1, '2025-01-01 00:00:00', '2025-12-31 00:00:00'),
        (4, 18, 2, '2025-02-01 00:00:00', '2025-06-01 00:00:00'),
        (5, 19, 1, '2025-01-01 00:00:00', None),
        (1, 20, 2, '2025-03-01 00:00:00', '2025-12-31 00:00:00')
    ]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for user_id, keyword_id, subscription_type_id, start_date, end_date in subscriptions:
        cursor.execute(
            "INSERT INTO user_subscription (user_id, keyword_id, subscription_type_id, start_date, end_date) "
            "VALUES (%s, %s, %s, %s, %s)",
            (user_id, keyword_id, subscription_type_id, start_date, end_date)
        )
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Generated {len(subscriptions)} subscriptions")

def main():
    print("Waiting for database to be ready...")
    time.sleep(5)  # Give some time for the database to initialize
    
    print("Generating keywords...")
    keywords = generate_keywords()

    print("Generating search volume data...")
    generate_search_volumes(keywords)
    
    print("Generating users...")
    generate_users()
    
    print("Generating subscriptions...")
    generate_subscriptions()
    
    print("Data generation complete!")

if __name__ == "__main__":
    main()