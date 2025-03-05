-- Create database
CREATE DATABASE IF NOT EXISTS analytics;

-- Use analytics database
USE analytics;

-- Web analytics events table
CREATE TABLE IF NOT EXISTS events (
    timestamp DateTime64(3),
    user_id String,
    session_id String,
    event_type String,
    page_url String,
    referrer String,
    user_agent String,
    ip_address String,
    country String,
    device_type String,
    browser String,
    os String,
    screen_width UInt16,
    screen_height UInt16,
    duration UInt32,
    value Float64
) ENGINE = MergeTree()
ORDER BY (timestamp, user_id)
PARTITION BY toYYYYMM(timestamp)
SETTINGS index_granularity = 8192;

-- IoT sensor data table
CREATE TABLE IF NOT EXISTS sensor_data (
    timestamp DateTime64(3),
    sensor_id String,
    sensor_type String,
    location String,
    value Float64,
    unit String,
    status String,
    battery_level Float32,
    signal_strength Int8,
    temperature Float32,
    humidity Float32,
    metadata String
) ENGINE = MergeTree()
ORDER BY (timestamp, sensor_id)
PARTITION BY toYYYYMM(timestamp)
SETTINGS index_granularity = 8192;

-- Financial transactions table
CREATE TABLE IF NOT EXISTS transactions (
    timestamp DateTime64(3),
    transaction_id String,
    user_id String,
    account_id String,
    transaction_type String,
    amount Decimal(18, 2),
    currency String,
    merchant_id String,
    merchant_category String,
    payment_method String,
    location String,
    latitude Float64,
    longitude Float64,
    status String,
    risk_score Float32,
    fraud_flag UInt8
) ENGINE = MergeTree()
ORDER BY (timestamp, user_id)
PARTITION BY toYYYYMM(timestamp)
SETTINGS index_granularity = 8192;

-- Application logs table
CREATE TABLE IF NOT EXISTS app_logs (
    timestamp DateTime64(3),
    log_level String,
    application String,
    service String,
    instance_id String,
    message String,
    user_id String,
    session_id String,
    request_id String,
    duration_ms UInt32,
    status_code UInt16,
    ip_address String,
    user_agent String,
    error_code String,
    stack_trace String
) ENGINE = MergeTree()
ORDER BY (timestamp, application, service)
PARTITION BY toYYYYMM(timestamp)
SETTINGS index_granularity = 8192;

-- E-commerce orders table
CREATE TABLE IF NOT EXISTS orders (
    timestamp DateTime64(3),
    order_id String,
    user_id String,
    product_id String,
    product_name String,
    category String,
    quantity UInt32,
    unit_price Decimal(10, 2),
    total_amount Decimal(18, 2),
    discount_amount Decimal(10, 2),
    tax_amount Decimal(10, 2),
    shipping_amount Decimal(10, 2),
    payment_method String,
    shipping_address String,
    billing_address String,
    order_status String,
    fulfillment_status String
) ENGINE = MergeTree()
ORDER BY (timestamp, user_id)
PARTITION BY toYYYYMM(timestamp)
SETTINGS index_granularity = 8192;

-- Create materialized views for real-time analytics

-- Hourly events summary
CREATE MATERIALIZED VIEW IF NOT EXISTS events_hourly
ENGINE = SummingMergeTree()
ORDER BY (hour, event_type, country)
AS SELECT
    toStartOfHour(timestamp) as hour,
    event_type,
    country,
    count() as events_count,
    uniq(user_id) as unique_users,
    uniq(session_id) as unique_sessions,
    avg(duration) as avg_duration,
    sum(value) as total_value
FROM events
GROUP BY hour, event_type, country;

-- Daily user analytics
CREATE MATERIALIZED VIEW IF NOT EXISTS user_analytics_daily
ENGINE = ReplacingMergeTree()
ORDER BY (date, user_id)
AS SELECT
    toDate(timestamp) as date,
    user_id,
    count() as total_events,
    uniq(session_id) as sessions_count,
    sum(duration) as total_duration,
    sum(value) as total_value,
    min(timestamp) as first_event,
    max(timestamp) as last_event
FROM events
GROUP BY date, user_id;

-- Sensor data hourly aggregations
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_data_hourly
ENGINE = SummingMergeTree()
ORDER BY (hour, sensor_id, sensor_type)
AS SELECT
    toStartOfHour(timestamp) as hour,
    sensor_id,
    sensor_type,
    location,
    count() as readings_count,
    avg(value) as avg_value,
    min(value) as min_value,
    max(value) as max_value,
    stddevPop(value) as stddev_value,
    avg(battery_level) as avg_battery_level,
    avg(signal_strength) as avg_signal_strength
FROM sensor_data
GROUP BY hour, sensor_id, sensor_type, location;

-- Transaction risk analysis
CREATE MATERIALIZED VIEW IF NOT EXISTS transaction_risk_hourly
ENGINE = SummingMergeTree()
ORDER BY (hour, merchant_category, location)
AS SELECT
    toStartOfHour(timestamp) as hour,
    merchant_category,
    location,
    count() as transaction_count,
    sum(amount) as total_amount,
    avg(amount) as avg_amount,
    avg(risk_score) as avg_risk_score,
    sum(fraud_flag) as fraud_count,
    uniq(user_id) as unique_users
FROM transactions
GROUP BY hour, merchant_category, location;

-- Create dictionaries for lookups
CREATE DICTIONARY IF NOT EXISTS country_codes (
    code String,
    name String,
    continent String,
    region String
)
PRIMARY KEY code
SOURCE(CLICKHOUSE(
    HOST 'localhost' PORT 9000 USER 'default' TABLE 'country_reference' DB 'analytics'
))
LIFETIME(MIN 3600 MAX 7200)
LAYOUT(HASHED());

-- Create functions for common analytics patterns
CREATE OR REPLACE FUNCTION calculate_conversion_rate AS (events_count, conversions_count) -> 
    if(events_count > 0, conversions_count / events_count * 100, 0);

CREATE OR REPLACE FUNCTION detect_anomaly AS (current_value, historical_avg, threshold) -> 
    if(abs(current_value - historical_avg) > threshold, 1, 0);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_events_user_id ON events(user_id) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_events_page_url ON events(page_url) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_sensor_location ON sensor_data(location) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_transaction_merchant ON transactions(merchant_id) TYPE bloom_filter GRANULARITY 1;