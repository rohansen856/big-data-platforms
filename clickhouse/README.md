# ClickHouse Demo

This demo showcases ClickHouse's columnar analytics capabilities for high-speed data processing and analytics.

## Features

- **Columnar Storage**: Optimized for analytical queries
- **Real-time Analytics**: Sub-second query performance
- **Time Series Analysis**: Efficient time-based aggregations
- **Materialized Views**: Pre-computed aggregations
- **Compression**: Efficient data storage
- **Horizontal Scaling**: Distributed processing

## Quick Start

```bash
# Start ClickHouse
docker-compose up -d

# Access ClickHouse client
docker-compose exec clickhouse clickhouse-client

# Run analytics queries
docker-compose exec clickhouse python /app/analytics_demo.py

# Load sample data
docker-compose exec clickhouse python /app/data_loader.py
```

## Architecture

- **ClickHouse Server**: Main database server
- **ClickHouse Client**: Command-line interface
- **Tabix**: Web-based SQL editor
- **Data Generator**: Creates synthetic datasets

## Use Cases

- **Web Analytics**: Page views, user sessions, conversion tracking
- **IoT Analytics**: Sensor data processing and monitoring
- **Financial Analytics**: Transaction processing and risk analysis
- **Log Analysis**: Application and system log processing

## Ports

- 8123: ClickHouse HTTP interface
- 9000: ClickHouse TCP interface
- 8080: Tabix web interface

## Sample Queries

```sql
-- Real-time analytics
SELECT 
    toStartOfHour(timestamp) as hour,
    count() as events,
    uniq(user_id) as unique_users
FROM events 
WHERE timestamp >= now() - INTERVAL 1 DAY
GROUP BY hour
ORDER BY hour;

-- Time series analysis
SELECT 
    toStartOfMinute(timestamp) as minute,
    avg(value) as avg_value,
    max(value) as max_value
FROM sensor_data
WHERE timestamp >= now() - INTERVAL 1 HOUR
GROUP BY minute
ORDER BY minute;
```