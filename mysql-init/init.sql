-- Create the database schema
CREATE DATABASE IF NOT EXISTS search_analytics;
CREATE DATABASE IF NOT EXISTS airflow;
USE search_analytics;

-- Original tables
CREATE TABLE keyword (
    keyword_id BIGINT PRIMARY KEY,
    keyword_name VARCHAR(255) NOT NULL
);

CREATE TABLE keyword_search_volume (
    keyword_id BIGINT,
    created_datetime DATETIME,  -- Hourly format - yyyy-MM-dd HH:00:00
    search_volume BIGINT,
    PRIMARY KEY (keyword_id, created_datetime),
    FOREIGN KEY (keyword_id) REFERENCES keyword(keyword_id)
);

-- New tables for subscription system
CREATE TABLE user (
    user_id BIGINT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL
);

CREATE TABLE subscription_type (
    subscription_type_id INT PRIMARY KEY,
    subscription_name ENUM('hourly', 'daily') NOT NULL
);

-- Insert default subscription types
INSERT INTO subscription_type (subscription_type_id, subscription_name) VALUES (1, 'hourly');
INSERT INTO subscription_type (subscription_type_id, subscription_name) VALUES (2, 'daily');

CREATE TABLE user_subscription (
    subscription_id BIGINT AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    keyword_id BIGINT NOT NULL,
    subscription_type_id INT NOT NULL,
    start_date DATETIME NOT NULL,
    end_date DATETIME,
    PRIMARY KEY (subscription_id),  
    UNIQUE KEY uk_user_keyword_start (user_id, keyword_id, start_date),
    FOREIGN KEY (user_id) REFERENCES user(user_id),
    FOREIGN KEY (keyword_id) REFERENCES keyword(keyword_id),
    FOREIGN KEY (subscription_type_id) REFERENCES subscription_type(subscription_type_id)
);

-- Create view for daily data (9:00 AM data or nearest time)
-- CREATE VIEW daily_keyword_search_volume AS
-- WITH ranked_data AS (
--     SELECT 
--         k.keyword_id,
--         k.keyword_name,
--         DATE(ksv.created_datetime) AS date,
--         ksv.created_datetime,
--         ksv.search_volume,
--         ABS(TIMESTAMPDIFF(HOUR, ksv.created_datetime, 
--              TIMESTAMP(DATE(ksv.created_datetime), '09:00:00'))) AS time_diff,
--         RANK() OVER (
--             PARTITION BY k.keyword_id, DATE(ksv.created_datetime) 
--             ORDER BY ABS(TIMESTAMPDIFF(HOUR, ksv.created_datetime, 
--                       TIMESTAMP(DATE(ksv.created_datetime), '09:00:00')))
--         ) AS rank_by_time_diff
--     FROM 
--         keyword k
--     JOIN 
--         keyword_search_volume ksv ON k.keyword_id = ksv.keyword_id
-- )
-- SELECT 
--     keyword_id,
--     date,
--     created_datetime,
--     search_volume
-- FROM 
--     ranked_data
-- WHERE 
--     rank_by_time_diff = 1;  

-- Create new table for daily aggregated data
CREATE TABLE daily_keyword_search_volume (
    keyword_id BIGINT,
    created_datetime DATETIME,
    search_volume BIGINT,
    date DATE,
    PRIMARY KEY (keyword_id, date),
    FOREIGN KEY (keyword_id) REFERENCES keyword(keyword_id)
);