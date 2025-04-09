-- Sample Pig Latin script for data analysis
-- This script demonstrates basic Pig operations

-- Register UDFs if needed
-- REGISTER /path/to/custom/udf.jar;

-- Load data from CSV file
raw_data = LOAD '/shared-data/sample_data.csv' 
    USING PigStorage(',') 
    AS (
        timestamp:chararray,
        event_type:chararray,
        user_id:chararray,
        page_url:chararray,
        session_id:chararray,
        user_agent:chararray,
        ip_address:chararray,
        country:chararray,
        value:int
    );

-- Filter out invalid records
clean_data = FILTER raw_data BY 
    timestamp IS NOT NULL AND 
    event_type IS NOT NULL AND 
    user_id IS NOT NULL;

-- Group by event type
grouped_by_event = GROUP clean_data BY event_type;

-- Calculate aggregations
event_stats = FOREACH grouped_by_event GENERATE 
    group AS event_type,
    COUNT(clean_data) AS total_events,
    SUM(clean_data.value) AS total_value,
    AVG(clean_data.value) AS avg_value,
    MIN(clean_data.value) AS min_value,
    MAX(clean_data.value) AS max_value;

-- Sort by total events descending
sorted_stats = ORDER event_stats BY total_events DESC;

-- Store results
STORE sorted_stats INTO '/shared-data/pig_output/event_analysis' 
    USING PigStorage(',');

-- User analysis
user_activity = GROUP clean_data BY user_id;
user_stats = FOREACH user_activity GENERATE 
    group AS user_id,
    COUNT(clean_data) AS session_count,
    COUNT(DISTINCT clean_data.session_id) AS unique_sessions,
    SUM(clean_data.value) AS total_value;

-- Filter active users (more than 5 events)
active_users = FILTER user_stats BY session_count > 5;

-- Store user analysis
STORE active_users INTO '/shared-data/pig_output/user_analysis' 
    USING PigStorage(',');

-- Country analysis
country_stats = GROUP clean_data BY country;
country_analysis = FOREACH country_stats GENERATE 
    group AS country,
    COUNT(clean_data) AS total_events,
    COUNT(DISTINCT clean_data.user_id) AS unique_users,
    AVG(clean_data.value) AS avg_value;

-- Sort by total events
sorted_countries = ORDER country_analysis BY total_events DESC;

-- Store country analysis
STORE sorted_countries INTO '/shared-data/pig_output/country_analysis' 
    USING PigStorage(',');

-- Page popularity analysis
page_stats = GROUP clean_data BY page_url;
page_analysis = FOREACH page_stats GENERATE 
    group AS page_url,
    COUNT(clean_data) AS page_views,
    COUNT(DISTINCT clean_data.user_id) AS unique_visitors,
    AVG(clean_data.value) AS avg_engagement;

-- Top 10 pages by views
top_pages = ORDER page_analysis BY page_views DESC;
top_10_pages = LIMIT top_pages 10;

-- Store page analysis
STORE top_10_pages INTO '/shared-data/pig_output/top_pages' 
    USING PigStorage(',');

-- Display sample results
DUMP sorted_stats;