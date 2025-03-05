#!/usr/bin/env python3

import time
import os
from clickhouse_driver import Client
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClickHouseAnalytics:
    def __init__(self, host='localhost', port=9000):
        self.client = Client(host=host, port=port)
        
        # Test connection
        try:
            self.client.execute('SELECT 1')
            logger.info("Connected to ClickHouse successfully")
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def run_web_analytics(self):
        """Run web analytics queries"""
        logger.info("=== Web Analytics Demo ===")
        
        # Real-time page views
        logger.info("1. Real-time page views (last hour)")
        query = """
        SELECT 
            toStartOfMinute(timestamp) as minute,
            page_url,
            count() as page_views,
            uniq(user_id) as unique_users
        FROM analytics.events 
        WHERE timestamp >= now() - INTERVAL 1 HOUR
        GROUP BY minute, page_url
        ORDER BY minute DESC, page_views DESC
        LIMIT 20
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=['minute', 'page_url', 'page_views', 'unique_users'])
        print(df.to_string(index=False))
        
        # Traffic by country
        logger.info("\n2. Traffic by country (last 24 hours)")
        query = """
        SELECT 
            country,
            count() as total_events,
            uniq(user_id) as unique_users,
            uniq(session_id) as sessions,
            avg(duration) as avg_duration
        FROM analytics.events 
        WHERE timestamp >= now() - INTERVAL 24 HOUR
        GROUP BY country
        ORDER BY total_events DESC
        LIMIT 10
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=['country', 'total_events', 'unique_users', 'sessions', 'avg_duration'])
        print(df.to_string(index=False))
        
        # Conversion funnel
        logger.info("\n3. Conversion funnel analysis")
        query = """
        SELECT 
            event_type,
            count() as events,
            uniq(user_id) as unique_users,
            round(events * 100.0 / (SELECT count() FROM analytics.events WHERE timestamp >= now() - INTERVAL 24 HOUR), 2) as percentage
        FROM analytics.events 
        WHERE timestamp >= now() - INTERVAL 24 HOUR
        GROUP BY event_type
        ORDER BY events DESC
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=['event_type', 'events', 'unique_users', 'percentage'])
        print(df.to_string(index=False))
        
        # Device and browser analytics
        logger.info("\n4. Device and browser analytics")
        query = """
        SELECT 
            device_type,
            browser,
            count() as events,
            uniq(user_id) as unique_users,
            avg(duration) as avg_duration
        FROM analytics.events 
        WHERE timestamp >= now() - INTERVAL 24 HOUR
        GROUP BY device_type, browser
        ORDER BY events DESC
        LIMIT 15
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=['device_type', 'browser', 'events', 'unique_users', 'avg_duration'])
        print(df.to_string(index=False))
    
    def run_sensor_analytics(self):
        """Run IoT sensor analytics"""
        logger.info("\n=== IoT Sensor Analytics Demo ===")
        
        # Current sensor readings
        logger.info("1. Current sensor readings")
        query = """
        SELECT 
            sensor_id,
            sensor_type,
            location,
            argMax(value, timestamp) as latest_value,
            argMax(unit, timestamp) as unit,
            argMax(status, timestamp) as status,
            argMax(battery_level, timestamp) as battery_level,
            max(timestamp) as last_reading
        FROM analytics.sensor_data
        WHERE timestamp >= now() - INTERVAL 1 HOUR
        GROUP BY sensor_id, sensor_type, location
        ORDER BY sensor_id
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=['sensor_id', 'sensor_type', 'location', 'latest_value', 'unit', 'status', 'battery_level', 'last_reading'])
        print(df.to_string(index=False))
        
        # Sensor trends
        logger.info("\n2. Sensor value trends (last 2 hours)")
        query = """
        SELECT 
            toStartOfMinute(timestamp) as minute,
            sensor_id,
            sensor_type,
            avg(value) as avg_value,
            min(value) as min_value,
            max(value) as max_value,
            stddevPop(value) as stddev_value
        FROM analytics.sensor_data 
        WHERE timestamp >= now() - INTERVAL 2 HOUR
        GROUP BY minute, sensor_id, sensor_type
        ORDER BY minute DESC, sensor_id
        LIMIT 30
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=['minute', 'sensor_id', 'sensor_type', 'avg_value', 'min_value', 'max_value', 'stddev_value'])
        print(df.to_string(index=False))
        
        # Anomaly detection
        logger.info("\n3. Anomaly detection")
        query = """
        SELECT 
            sensor_id,
            sensor_type,
            location,
            count() as total_readings,
            sum(status = 'anomaly') as anomaly_count,
            round(anomaly_count * 100.0 / total_readings, 2) as anomaly_percentage,
            avg(value) as avg_value,
            stddevPop(value) as stddev_value
        FROM analytics.sensor_data 
        WHERE timestamp >= now() - INTERVAL 24 HOUR
        GROUP BY sensor_id, sensor_type, location
        HAVING anomaly_count > 0
        ORDER BY anomaly_percentage DESC
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=['sensor_id', 'sensor_type', 'location', 'total_readings', 'anomaly_count', 'anomaly_percentage', 'avg_value', 'stddev_value'])
        print(df.to_string(index=False))
    
    def run_transaction_analytics(self):
        """Run financial transaction analytics"""
        logger.info("\n=== Financial Transaction Analytics Demo ===")
        
        # Transaction volume and value
        logger.info("1. Transaction volume and value by hour")
        query = """
        SELECT 
            toStartOfHour(timestamp) as hour,
            count() as transaction_count,
            sum(amount) as total_value,
            avg(amount) as avg_value,
            uniq(user_id) as unique_users,
            sum(fraud_flag) as fraud_count
        FROM analytics.transactions 
        WHERE timestamp >= now() - INTERVAL 24 HOUR
        GROUP BY hour
        ORDER BY hour DESC
        LIMIT 24
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=['hour', 'transaction_count', 'total_value', 'avg_value', 'unique_users', 'fraud_count'])
        print(df.to_string(index=False))
        
        # Fraud analysis
        logger.info("\n2. Fraud analysis by merchant category")
        query = """
        SELECT 
            merchant_category,
            count() as total_transactions,
            sum(fraud_flag) as fraud_count,
            round(fraud_count * 100.0 / total_transactions, 2) as fraud_percentage,
            avg(risk_score) as avg_risk_score,
            sum(amount) as total_amount
        FROM analytics.transactions 
        WHERE timestamp >= now() - INTERVAL 24 HOUR
        GROUP BY merchant_category
        ORDER BY fraud_percentage DESC
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=['merchant_category', 'total_transactions', 'fraud_count', 'fraud_percentage', 'avg_risk_score', 'total_amount'])
        print(df.to_string(index=False))
        
        # Geographic analysis
        logger.info("\n3. Transaction analysis by location")
        query = """
        SELECT 
            location,
            count() as transaction_count,
            sum(amount) as total_amount,
            avg(amount) as avg_amount,
            uniq(user_id) as unique_users,
            avg(risk_score) as avg_risk_score
        FROM analytics.transactions 
        WHERE timestamp >= now() - INTERVAL 24 HOUR
        GROUP BY location
        ORDER BY transaction_count DESC
        LIMIT 10
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=['location', 'transaction_count', 'total_amount', 'avg_amount', 'unique_users', 'avg_risk_score'])
        print(df.to_string(index=False))
    
    def run_log_analytics(self):
        """Run application log analytics"""
        logger.info("\n=== Application Log Analytics Demo ===")
        
        # Error rate analysis
        logger.info("1. Error rate by application and service")
        query = """
        SELECT 
            application,
            service,
            count() as total_logs,
            sum(log_level = 'ERROR') as error_count,
            sum(log_level = 'WARN') as warn_count,
            round(error_count * 100.0 / total_logs, 2) as error_percentage,
            round(warn_count * 100.0 / total_logs, 2) as warn_percentage
        FROM analytics.app_logs 
        WHERE timestamp >= now() - INTERVAL 2 HOUR
        GROUP BY application, service
        ORDER BY error_percentage DESC
        LIMIT 15
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=['application', 'service', 'total_logs', 'error_count', 'warn_count', 'error_percentage', 'warn_percentage'])
        print(df.to_string(index=False))
        
        # Performance analysis
        logger.info("\n2. Performance analysis (response times)")
        query = """
        SELECT 
            application,
            service,
            count() as request_count,
            avg(duration_ms) as avg_duration,
            quantile(0.5)(duration_ms) as median_duration,
            quantile(0.95)(duration_ms) as p95_duration,
            quantile(0.99)(duration_ms) as p99_duration,
            max(duration_ms) as max_duration
        FROM analytics.app_logs 
        WHERE timestamp >= now() - INTERVAL 2 HOUR
        AND duration_ms > 0
        GROUP BY application, service
        ORDER BY avg_duration DESC
        LIMIT 10
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=['application', 'service', 'request_count', 'avg_duration', 'median_duration', 'p95_duration', 'p99_duration', 'max_duration'])
        print(df.to_string(index=False))
        
        # Status code analysis
        logger.info("\n3. HTTP status code distribution")
        query = """
        SELECT 
            status_code,
            count() as count,
            round(count * 100.0 / (SELECT count() FROM analytics.app_logs WHERE timestamp >= now() - INTERVAL 2 HOUR AND status_code > 0), 2) as percentage
        FROM analytics.app_logs 
        WHERE timestamp >= now() - INTERVAL 2 HOUR
        AND status_code > 0
        GROUP BY status_code
        ORDER BY count DESC
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=['status_code', 'count', 'percentage'])
        print(df.to_string(index=False))
    
    def run_ecommerce_analytics(self):
        """Run e-commerce analytics"""
        logger.info("\n=== E-commerce Analytics Demo ===")
        
        # Sales performance
        logger.info("1. Sales performance by product category")
        query = """
        SELECT 
            category,
            count() as order_count,
            sum(quantity) as total_quantity,
            sum(total_amount) as total_revenue,
            avg(total_amount) as avg_order_value,
            uniq(user_id) as unique_customers
        FROM analytics.orders 
        WHERE timestamp >= now() - INTERVAL 24 HOUR
        GROUP BY category
        ORDER BY total_revenue DESC
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=['category', 'order_count', 'total_quantity', 'total_revenue', 'avg_order_value', 'unique_customers'])
        print(df.to_string(index=False))
        
        # Hourly sales trends
        logger.info("\n2. Hourly sales trends")
        query = """
        SELECT 
            toStartOfHour(timestamp) as hour,
            count() as orders,
            sum(total_amount) as revenue,
            avg(total_amount) as avg_order_value,
            uniq(user_id) as unique_customers
        FROM analytics.orders 
        WHERE timestamp >= now() - INTERVAL 24 HOUR
        GROUP BY hour
        ORDER BY hour DESC
        LIMIT 24
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=['hour', 'orders', 'revenue', 'avg_order_value', 'unique_customers'])
        print(df.to_string(index=False))
        
        # Order status analysis
        logger.info("\n3. Order status distribution")
        query = """
        SELECT 
            order_status,
            fulfillment_status,
            count() as order_count,
            sum(total_amount) as total_revenue,
            avg(total_amount) as avg_order_value
        FROM analytics.orders 
        WHERE timestamp >= now() - INTERVAL 24 HOUR
        GROUP BY order_status, fulfillment_status
        ORDER BY order_count DESC
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=['order_status', 'fulfillment_status', 'order_count', 'total_revenue', 'avg_order_value'])
        print(df.to_string(index=False))
    
    def run_materialized_view_demo(self):
        """Demonstrate materialized views"""
        logger.info("\n=== Materialized Views Demo ===")
        
        # Query materialized view for events
        logger.info("1. Hourly events summary (from materialized view)")
        query = """
        SELECT 
            hour,
            event_type,
            country,
            events_count,
            unique_users,
            unique_sessions,
            round(avg_duration, 2) as avg_duration
        FROM analytics.events_hourly 
        WHERE hour >= now() - INTERVAL 24 HOUR
        ORDER BY hour DESC, events_count DESC
        LIMIT 20
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=['hour', 'event_type', 'country', 'events_count', 'unique_users', 'unique_sessions', 'avg_duration'])
        print(df.to_string(index=False))
        
        # Query materialized view for sensor data
        logger.info("\n2. Sensor data hourly aggregations")
        query = """
        SELECT 
            hour,
            sensor_type,
            location,
            readings_count,
            round(avg_value, 2) as avg_value,
            round(min_value, 2) as min_value,
            round(max_value, 2) as max_value,
            round(avg_battery_level, 1) as avg_battery_level
        FROM analytics.sensor_data_hourly 
        WHERE hour >= now() - INTERVAL 12 HOUR
        ORDER BY hour DESC, sensor_type
        LIMIT 15
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=['hour', 'sensor_type', 'location', 'readings_count', 'avg_value', 'min_value', 'max_value', 'avg_battery_level'])
        print(df.to_string(index=False))
    
    def run_performance_demo(self):
        """Demonstrate query performance"""
        logger.info("\n=== Query Performance Demo ===")
        
        # Fast aggregation query
        logger.info("1. Fast aggregation query (millions of rows)")
        start_time = time.time()
        
        query = """
        SELECT 
            count() as total_events,
            uniq(user_id) as unique_users,
            uniq(session_id) as unique_sessions,
            avg(duration) as avg_duration,
            sum(value) as total_value
        FROM analytics.events 
        WHERE timestamp >= now() - INTERVAL 24 HOUR
        """
        
        result = self.client.execute(query)
        end_time = time.time()
        
        df = pd.DataFrame(result, columns=['total_events', 'unique_users', 'unique_sessions', 'avg_duration', 'total_value'])
        print(df.to_string(index=False))
        print(f"Query executed in {end_time - start_time:.3f} seconds")
        
        # Complex analytical query
        logger.info("\n2. Complex analytical query")
        start_time = time.time()
        
        query = """
        SELECT 
            event_type,
            country,
            device_type,
            count() as events,
            uniq(user_id) as unique_users,
            avg(duration) as avg_duration,
            quantile(0.5)(duration) as median_duration,
            quantile(0.95)(duration) as p95_duration
        FROM analytics.events 
        WHERE timestamp >= now() - INTERVAL 24 HOUR
        GROUP BY event_type, country, device_type
        HAVING events > 10
        ORDER BY events DESC
        LIMIT 20
        """
        
        result = self.client.execute(query)
        end_time = time.time()
        
        df = pd.DataFrame(result, columns=['event_type', 'country', 'device_type', 'events', 'unique_users', 'avg_duration', 'median_duration', 'p95_duration'])
        print(df.to_string(index=False))
        print(f"Query executed in {end_time - start_time:.3f} seconds")

def main():
    # Get connection details from environment
    host = os.getenv('CLICKHOUSE_HOST', 'localhost')
    port = int(os.getenv('CLICKHOUSE_PORT', '9000'))
    
    # Wait for data to be generated
    logger.info("Waiting for data to be generated...")
    time.sleep(60)
    
    try:
        analytics = ClickHouseAnalytics(host=host, port=port)
        
        # Run all analytics demos
        analytics.run_web_analytics()
        analytics.run_sensor_analytics()
        analytics.run_transaction_analytics()
        analytics.run_log_analytics()
        analytics.run_ecommerce_analytics()
        analytics.run_materialized_view_demo()
        analytics.run_performance_demo()
        
        logger.info("\n=== Analytics Demo Complete ===")
        
    except Exception as e:
        logger.error(f"Error running analytics: {e}")

if __name__ == "__main__":
    main()