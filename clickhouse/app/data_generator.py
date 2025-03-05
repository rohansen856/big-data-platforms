#!/usr/bin/env python3

import time
import random
import json
from datetime import datetime, timedelta
from clickhouse_driver import Client
from faker import Faker
import threading
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

class ClickHouseDataGenerator:
    def __init__(self, host='localhost', port=8123):
        self.client = Client(host=host, port=port)
        self.running = True
        
        # Test connection
        try:
            self.client.execute('SELECT 1')
            logger.info("Connected to ClickHouse successfully")
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def generate_web_events(self, batch_size=100):
        """Generate web analytics events"""
        logger.info("Starting web events generation")
        
        event_types = ['page_view', 'click', 'form_submit', 'purchase', 'login', 'logout', 'search']
        pages = ['/home', '/products', '/about', '/contact', '/cart', '/checkout', '/profile', '/search']
        countries = ['US', 'UK', 'DE', 'FR', 'JP', 'CA', 'AU', 'BR', 'IN', 'CN']
        devices = ['desktop', 'mobile', 'tablet']
        browsers = ['Chrome', 'Firefox', 'Safari', 'Edge']
        operating_systems = ['Windows', 'macOS', 'Linux', 'iOS', 'Android']
        
        while self.running:
            try:
                events = []
                
                for _ in range(batch_size):
                    event = {
                        'timestamp': datetime.now() - timedelta(seconds=random.randint(0, 3600)),
                        'user_id': fake.uuid4(),
                        'session_id': fake.uuid4(),
                        'event_type': random.choice(event_types),
                        'page_url': random.choice(pages),
                        'referrer': random.choice(['google.com', 'facebook.com', 'twitter.com', 'direct', '']),
                        'user_agent': fake.user_agent(),
                        'ip_address': fake.ipv4(),
                        'country': random.choice(countries),
                        'device_type': random.choice(devices),
                        'browser': random.choice(browsers),
                        'os': random.choice(operating_systems),
                        'screen_width': random.choice([1920, 1366, 1536, 1440, 1280]),
                        'screen_height': random.choice([1080, 768, 864, 900, 720]),
                        'duration': random.randint(1, 300),
                        'value': round(random.uniform(0, 100), 2)
                    }
                    events.append(event)
                
                # Insert batch
                self.client.execute(
                    'INSERT INTO analytics.events VALUES',
                    events
                )
                
                logger.info(f"Inserted {len(events)} web events")
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"Error generating web events: {e}")
                time.sleep(10)
    
    def generate_sensor_data(self, batch_size=50):
        """Generate IoT sensor data"""
        logger.info("Starting sensor data generation")
        
        sensors = [
            {'id': 'TEMP-001', 'type': 'temperature', 'location': 'Warehouse-A'},
            {'id': 'TEMP-002', 'type': 'temperature', 'location': 'Warehouse-B'},
            {'id': 'HUM-001', 'type': 'humidity', 'location': 'Warehouse-A'},
            {'id': 'HUM-002', 'type': 'humidity', 'location': 'Warehouse-B'},
            {'id': 'PRESS-001', 'type': 'pressure', 'location': 'Factory-1'},
            {'id': 'PRESS-002', 'type': 'pressure', 'location': 'Factory-2'},
            {'id': 'VIBR-001', 'type': 'vibration', 'location': 'Machine-1'},
            {'id': 'VIBR-002', 'type': 'vibration', 'location': 'Machine-2'}
        ]
        
        while self.running:
            try:
                readings = []
                
                for _ in range(batch_size):
                    sensor = random.choice(sensors)
                    
                    # Generate realistic values based on sensor type
                    if sensor['type'] == 'temperature':
                        value = round(random.uniform(15.0, 35.0), 2)
                        unit = '°C'
                    elif sensor['type'] == 'humidity':
                        value = round(random.uniform(30.0, 80.0), 2)
                        unit = '%'
                    elif sensor['type'] == 'pressure':
                        value = round(random.uniform(1000.0, 1050.0), 2)
                        unit = 'hPa'
                    elif sensor['type'] == 'vibration':
                        value = round(random.uniform(0.0, 10.0), 2)
                        unit = 'mm/s'
                    
                    # Add occasional anomalies
                    if random.random() < 0.05:
                        value *= random.uniform(1.5, 3.0)
                        status = 'anomaly'
                    else:
                        status = 'normal'
                    
                    reading = {
                        'timestamp': datetime.now() - timedelta(seconds=random.randint(0, 600)),
                        'sensor_id': sensor['id'],
                        'sensor_type': sensor['type'],
                        'location': sensor['location'],
                        'value': value,
                        'unit': unit,
                        'status': status,
                        'battery_level': round(random.uniform(20.0, 100.0), 1),
                        'signal_strength': random.randint(1, 5),
                        'temperature': round(random.uniform(20.0, 30.0), 1),
                        'humidity': round(random.uniform(40.0, 60.0), 1),
                        'metadata': json.dumps({'version': '1.0', 'calibration': 'auto'})
                    }
                    readings.append(reading)
                
                # Insert batch
                self.client.execute(
                    'INSERT INTO analytics.sensor_data VALUES',
                    readings
                )
                
                logger.info(f"Inserted {len(readings)} sensor readings")
                time.sleep(10)
                
            except Exception as e:
                logger.error(f"Error generating sensor data: {e}")
                time.sleep(10)
    
    def generate_transactions(self, batch_size=30):
        """Generate financial transactions"""
        logger.info("Starting transaction generation")
        
        transaction_types = ['purchase', 'transfer', 'withdrawal', 'deposit', 'payment']
        currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD']
        merchant_categories = ['grocery', 'gas', 'restaurant', 'retail', 'entertainment', 'utilities']
        payment_methods = ['credit_card', 'debit_card', 'paypal', 'bank_transfer', 'cash']
        locations = ['New York', 'London', 'Tokyo', 'Paris', 'Toronto', 'Sydney']
        
        while self.running:
            try:
                transactions = []
                
                for _ in range(batch_size):
                    amount = round(random.uniform(5.0, 1000.0), 2)
                    
                    # Generate risk score based on amount and other factors
                    risk_score = 0.0
                    if amount > 500:
                        risk_score += 0.3
                    if random.random() < 0.1:  # 10% chance of unusual pattern
                        risk_score += 0.4
                    
                    risk_score = min(risk_score + random.uniform(0, 0.3), 1.0)
                    
                    transaction = {
                        'timestamp': datetime.now() - timedelta(seconds=random.randint(0, 1800)),
                        'transaction_id': fake.uuid4(),
                        'user_id': fake.uuid4(),
                        'account_id': fake.uuid4(),
                        'transaction_type': random.choice(transaction_types),
                        'amount': amount,
                        'currency': random.choice(currencies),
                        'merchant_id': fake.uuid4(),
                        'merchant_category': random.choice(merchant_categories),
                        'payment_method': random.choice(payment_methods),
                        'location': random.choice(locations),
                        'latitude': round(random.uniform(-90, 90), 6),
                        'longitude': round(random.uniform(-180, 180), 6),
                        'status': random.choice(['completed', 'pending', 'failed']),
                        'risk_score': round(risk_score, 3),
                        'fraud_flag': 1 if risk_score > 0.7 else 0
                    }
                    transactions.append(transaction)
                
                # Insert batch
                self.client.execute(
                    'INSERT INTO analytics.transactions VALUES',
                    transactions
                )
                
                logger.info(f"Inserted {len(transactions)} transactions")
                time.sleep(15)
                
            except Exception as e:
                logger.error(f"Error generating transactions: {e}")
                time.sleep(10)
    
    def generate_app_logs(self, batch_size=200):
        """Generate application logs"""
        logger.info("Starting application logs generation")
        
        log_levels = ['INFO', 'WARN', 'ERROR', 'DEBUG']
        applications = ['web-app', 'mobile-app', 'api-gateway', 'auth-service', 'payment-service']
        services = ['user-service', 'product-service', 'order-service', 'notification-service']
        status_codes = [200, 201, 400, 401, 403, 404, 500, 502, 503]
        
        while self.running:
            try:
                logs = []
                
                for _ in range(batch_size):
                    log_level = random.choice(log_levels)
                    status_code = random.choice(status_codes)
                    
                    # Generate realistic log messages
                    if log_level == 'ERROR':
                        message = f"Database connection failed: {fake.sentence()}"
                        error_code = f"E{random.randint(1000, 9999)}"
                        stack_trace = f"Traceback: {fake.text(max_nb_chars=200)}"
                    elif log_level == 'WARN':
                        message = f"High memory usage detected: {fake.sentence()}"
                        error_code = f"W{random.randint(1000, 9999)}"
                        stack_trace = ''
                    else:
                        message = f"Request processed successfully: {fake.sentence()}"
                        error_code = ''
                        stack_trace = ''
                    
                    log_entry = {
                        'timestamp': datetime.now() - timedelta(seconds=random.randint(0, 300)),
                        'log_level': log_level,
                        'application': random.choice(applications),
                        'service': random.choice(services),
                        'instance_id': fake.uuid4(),
                        'message': message,
                        'user_id': fake.uuid4(),
                        'session_id': fake.uuid4(),
                        'request_id': fake.uuid4(),
                        'duration_ms': random.randint(10, 5000),
                        'status_code': status_code,
                        'ip_address': fake.ipv4(),
                        'user_agent': fake.user_agent(),
                        'error_code': error_code,
                        'stack_trace': stack_trace
                    }
                    logs.append(log_entry)
                
                # Insert batch
                self.client.execute(
                    'INSERT INTO analytics.app_logs VALUES',
                    logs
                )
                
                logger.info(f"Inserted {len(logs)} log entries")
                time.sleep(8)
                
            except Exception as e:
                logger.error(f"Error generating app logs: {e}")
                time.sleep(10)
    
    def generate_orders(self, batch_size=40):
        """Generate e-commerce orders"""
        logger.info("Starting orders generation")
        
        products = [
            {'id': 'PROD-001', 'name': 'Laptop', 'category': 'Electronics', 'price': 999.99},
            {'id': 'PROD-002', 'name': 'Mouse', 'category': 'Electronics', 'price': 29.99},
            {'id': 'PROD-003', 'name': 'T-Shirt', 'category': 'Clothing', 'price': 19.99},
            {'id': 'PROD-004', 'name': 'Book', 'category': 'Books', 'price': 12.99},
            {'id': 'PROD-005', 'name': 'Headphones', 'category': 'Electronics', 'price': 199.99}
        ]
        
        payment_methods = ['credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay']
        order_statuses = ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled']
        fulfillment_statuses = ['pending', 'processing', 'shipped', 'delivered', 'returned']
        
        while self.running:
            try:
                orders = []
                
                for _ in range(batch_size):
                    product = random.choice(products)
                    quantity = random.randint(1, 5)
                    unit_price = product['price']
                    
                    discount_amount = round(random.uniform(0, unit_price * 0.2), 2)
                    tax_amount = round((unit_price - discount_amount) * 0.08, 2)
                    shipping_amount = round(random.uniform(0, 15), 2)
                    total_amount = round(
                        (unit_price * quantity) - discount_amount + tax_amount + shipping_amount, 2
                    )
                    
                    order = {
                        'timestamp': datetime.now() - timedelta(seconds=random.randint(0, 7200)),
                        'order_id': fake.uuid4(),
                        'user_id': fake.uuid4(),
                        'product_id': product['id'],
                        'product_name': product['name'],
                        'category': product['category'],
                        'quantity': quantity,
                        'unit_price': unit_price,
                        'total_amount': total_amount,
                        'discount_amount': discount_amount,
                        'tax_amount': tax_amount,
                        'shipping_amount': shipping_amount,
                        'payment_method': random.choice(payment_methods),
                        'shipping_address': fake.address(),
                        'billing_address': fake.address(),
                        'order_status': random.choice(order_statuses),
                        'fulfillment_status': random.choice(fulfillment_statuses)
                    }
                    orders.append(order)
                
                # Insert batch
                self.client.execute(
                    'INSERT INTO analytics.orders VALUES',
                    orders
                )
                
                logger.info(f"Inserted {len(orders)} orders")
                time.sleep(12)
                
            except Exception as e:
                logger.error(f"Error generating orders: {e}")
                time.sleep(10)
    
    def stop(self):
        """Stop data generation"""
        self.running = False
        logger.info("Data generation stopped")

def main():
    # Get connection details from environment
    host = os.getenv('CLICKHOUSE_HOST', 'localhost')
    port = int(os.getenv('CLICKHOUSE_PORT', '9000'))
    
    # Wait for ClickHouse to be ready
    logger.info("Waiting for ClickHouse to be ready...")
    time.sleep(30)
    
    try:
        generator = ClickHouseDataGenerator(host=host, port=port)
        
        # Start data generation threads
        threads = [
            threading.Thread(target=generator.generate_web_events, daemon=True),
            threading.Thread(target=generator.generate_sensor_data, daemon=True),
            threading.Thread(target=generator.generate_transactions, daemon=True),
            threading.Thread(target=generator.generate_app_logs, daemon=True),
            threading.Thread(target=generator.generate_orders, daemon=True)
        ]
        
        for thread in threads:
            thread.start()
        
        logger.info("All data generators started. Press Ctrl+C to stop.")
        
        # Keep main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Shutting down data generators...")
        generator.stop()
    except Exception as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    main()