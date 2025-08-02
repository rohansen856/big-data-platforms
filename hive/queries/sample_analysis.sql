-- Sample Hive queries for data analysis

-- Create database
CREATE DATABASE IF NOT EXISTS analytics;
USE analytics;

-- Create external table for web events
CREATE EXTERNAL TABLE web_events (
    timestamp STRING,
    event_type STRING,
    user_id STRING,
    page_url STRING,
    session_id STRING,
    user_agent STRING,
    ip_address STRING,
    country STRING,
    value INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/shared-data/web_events/';

-- Event type analysis
SELECT 
    event_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT user_id) as unique_users,
    AVG(value) as avg_value,
    MAX(value) as max_value,
    MIN(value) as min_value
FROM web_events
GROUP BY event_type
ORDER BY total_events DESC;

-- Daily activity analysis
SELECT 
    DATE(timestamp) as date,
    COUNT(*) as daily_events,
    COUNT(DISTINCT user_id) as daily_users,
    COUNT(DISTINCT session_id) as daily_sessions
FROM web_events
GROUP BY DATE(timestamp)
ORDER BY date;

-- Country-wise analysis
SELECT 
    country,
    COUNT(*) as total_events,
    COUNT(DISTINCT user_id) as unique_users,
    AVG(value) as avg_engagement
FROM web_events
GROUP BY country
ORDER BY total_events DESC
LIMIT 10;

-- Page popularity analysis
SELECT 
    page_url,
    COUNT(*) as page_views,
    COUNT(DISTINCT user_id) as unique_visitors,
    AVG(value) as avg_engagement
FROM web_events
WHERE event_type = 'page_view'
GROUP BY page_url
ORDER BY page_views DESC
LIMIT 20;

-- User behavior analysis
SELECT 
    user_id,
    COUNT(*) as total_events,
    COUNT(DISTINCT session_id) as sessions,
    COUNT(DISTINCT page_url) as pages_visited,
    SUM(value) as total_value
FROM web_events
GROUP BY user_id
HAVING COUNT(*) > 10
ORDER BY total_events DESC
LIMIT 100;

-- Hourly activity patterns
SELECT 
    HOUR(timestamp) as hour,
    COUNT(*) as events_count,
    COUNT(DISTINCT user_id) as unique_users
FROM web_events
GROUP BY HOUR(timestamp)
ORDER BY hour;

-- Session analysis
SELECT 
    session_id,
    COUNT(*) as events_per_session,
    COUNT(DISTINCT page_url) as pages_per_session,
    MIN(timestamp) as session_start,
    MAX(timestamp) as session_end,
    SUM(value) as session_value
FROM web_events
GROUP BY session_id
ORDER BY events_per_session DESC
LIMIT 50;