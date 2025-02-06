#!/usr/bin/env python3

import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker
import threading
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

class EventProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            batch_size=16384,
            linger_ms=10,
            buffer_memory=33554432
        )
        
    def produce_orders(self, topic='orders'):
        """Produce e-commerce order events"""
        logger.info(f"Starting order producer for topic: {topic}")
        
        products = [
            {'id': 1, 'name': 'Laptop', 'price': 999.99, 'category': 'Electronics'},
            {'id': 2, 'name': 'Mouse', 'price': 29.99, 'category': 'Accessories'},
            {'id': 3, 'name': 'Keyboard', 'price': 79.99, 'category': 'Accessories'},
            {'id': 4, 'name': 'Monitor', 'price': 299.99, 'category': 'Electronics'},
            {'id': 5, 'name': 'Headphones', 'price': 199.99, 'category': 'Audio'},
            {'id': 6, 'name': 'Webcam', 'price': 89.99, 'category': 'Electronics'},
            {'id': 7, 'name': 'Speakers', 'price': 149.99, 'category': 'Audio'},
            {'id': 8, 'name': 'Tablet', 'price': 399.99, 'category': 'Electronics'}
        ]
        
        while True:
            try:
                product = random.choice(products)
                quantity = random.randint(1, 5)
                
                order = {
                    'order_id': fake.uuid4(),
                    'customer_id': fake.uuid4(),
                    'product_id': product['id'],
                    'product_name': product['name'],
                    'category': product['category'],
                    'quantity': quantity,
                    'unit_price': product['price'],
                    'total_amount': quantity * product['price'],
                    'customer_email': fake.email(),
                    'customer_city': fake.city(),
                    'customer_country': fake.country(),
                    'order_status': random.choice(['pending', 'confirmed', 'shipped', 'delivered']),
                    'payment_method': random.choice(['credit_card', 'debit_card', 'paypal', 'bank_transfer']),
                    'timestamp': datetime.now().isoformat(),
                    'event_type': 'order_created'
                }
                
                # Use customer_id as partition key for ordering
                key = order['customer_id']
                
                future = self.producer.send(topic, key=key, value=order)
                future.add_callback(lambda metadata: logger.debug(f"Order sent: {metadata.topic}:{metadata.partition}:{metadata.offset}"))
                future.add_errback(lambda e: logger.error(f"Failed to send order: {e}"))
                
                # Random delay between 0.5-2 seconds
                time.sleep(random.uniform(0.5, 2.0))
                
            except Exception as e:
                logger.error(f"Error producing order: {e}")
                time.sleep(1)
    
    def produce_clicks(self, topic='user-clicks'):
        """Produce user click events"""
        logger.info(f"Starting click producer for topic: {topic}")
        
        pages = ['/home', '/products', '/cart', '/checkout', '/profile', '/search', '/category/electronics', '/category/clothing']
        user_agents = ['Chrome', 'Firefox', 'Safari', 'Edge']
        
        while True:
            try:
                click = {
                    'user_id': fake.uuid4(),
                    'session_id': fake.uuid4(),
                    'page': random.choice(pages),
                    'action': random.choice(['click', 'view', 'scroll', 'hover']),
                    'element': random.choice(['button', 'link', 'image', 'text']),
                    'timestamp': datetime.now().isoformat(),
                    'ip_address': fake.ipv4(),
                    'user_agent': random.choice(user_agents),
                    'referrer': random.choice(['google.com', 'facebook.com', 'twitter.com', 'direct']),
                    'country': fake.country_code(),
                    'device_type': random.choice(['desktop', 'mobile', 'tablet']),
                    'event_type': 'user_click'
                }
                
                # Use session_id as partition key
                key = click['session_id']
                
                future = self.producer.send(topic, key=key, value=click)
                future.add_callback(lambda metadata: logger.debug(f"Click sent: {metadata.topic}:{metadata.partition}:{metadata.offset}"))
                future.add_errback(lambda e: logger.error(f"Failed to send click: {e}"))
                
                # High frequency - every 0.1-0.5 seconds
                time.sleep(random.uniform(0.1, 0.5))
                
            except Exception as e:
                logger.error(f"Error producing click: {e}")
                time.sleep(1)
    
    def produce_sensor_data(self, topic='iot-sensors'):
        """Produce IoT sensor data"""
        logger.info(f"Starting sensor producer for topic: {topic}")
        
        sensors = [
            {'sensor_id': 'temp-001', 'location': 'warehouse-a', 'type': 'temperature'},
            {'sensor_id': 'temp-002', 'location': 'warehouse-b', 'type': 'temperature'},
            {'sensor_id': 'hum-001', 'location': 'warehouse-a', 'type': 'humidity'},
            {'sensor_id': 'hum-002', 'location': 'warehouse-b', 'type': 'humidity'},
            {'sensor_id': 'press-001', 'location': 'factory-1', 'type': 'pressure'},
            {'sensor_id': 'press-002', 'location': 'factory-2', 'type': 'pressure'}
        ]
        
        while True:
            try:
                sensor = random.choice(sensors)
                
                # Generate realistic sensor values
                if sensor['type'] == 'temperature':
                    value = round(random.uniform(18.0, 25.0), 2)  # Celsius
                    unit = '°C'
                elif sensor['type'] == 'humidity':
                    value = round(random.uniform(40.0, 70.0), 2)  # Percentage
                    unit = '%'
                elif sensor['type'] == 'pressure':
                    value = round(random.uniform(1010.0, 1030.0), 2)  # hPa
                    unit = 'hPa'
                
                # Add some anomalies (5% chance)
                if random.random() < 0.05:
                    value *= random.uniform(1.5, 2.0)  # Anomaly
                    alert = True
                else:
                    alert = False
                
                reading = {
                    'sensor_id': sensor['sensor_id'],
                    'location': sensor['location'],
                    'sensor_type': sensor['type'],
                    'value': value,
                    'unit': unit,
                    'alert': alert,
                    'timestamp': datetime.now().isoformat(),
                    'battery_level': round(random.uniform(20.0, 100.0), 1),
                    'signal_strength': random.randint(1, 5),
                    'event_type': 'sensor_reading'
                }
                
                # Use sensor_id as partition key
                key = reading['sensor_id']
                
                future = self.producer.send(topic, key=key, value=reading)
                future.add_callback(lambda metadata: logger.debug(f"Sensor data sent: {metadata.topic}:{metadata.partition}:{metadata.offset}"))
                future.add_errback(lambda e: logger.error(f"Failed to send sensor data: {e}"))
                
                # Every 5-10 seconds
                time.sleep(random.uniform(5.0, 10.0))
                
            except Exception as e:
                logger.error(f"Error producing sensor data: {e}")
                time.sleep(1)
    
    def produce_transactions(self, topic='transactions'):
        """Produce financial transaction events"""
        logger.info(f"Starting transaction producer for topic: {topic}")
        
        while True:
            try:
                transaction_type = random.choice(['deposit', 'withdrawal', 'transfer', 'payment'])
                
                transaction = {
                    'transaction_id': fake.uuid4(),
                    'account_id': fake.uuid4(),
                    'user_id': fake.uuid4(),
                    'type': transaction_type,
                    'amount': round(random.uniform(10.0, 5000.0), 2),
                    'currency': random.choice(['USD', 'EUR', 'GBP', 'JPY']),
                    'merchant': fake.company() if transaction_type == 'payment' else None,
                    'category': random.choice(['food', 'transport', 'entertainment', 'utilities', 'shopping']),
                    'description': fake.sentence(nb_words=4),
                    'timestamp': datetime.now().isoformat(),
                    'status': random.choice(['pending', 'completed', 'failed']),
                    'location': fake.city(),
                    'event_type': 'transaction'
                }
                
                # Use account_id as partition key
                key = transaction['account_id']
                
                future = self.producer.send(topic, key=key, value=transaction)
                future.add_callback(lambda metadata: logger.debug(f"Transaction sent: {metadata.topic}:{metadata.partition}:{metadata.offset}"))
                future.add_errback(lambda e: logger.error(f"Failed to send transaction: {e}"))
                
                # Every 1-3 seconds
                time.sleep(random.uniform(1.0, 3.0))
                
            except Exception as e:
                logger.error(f"Error producing transaction: {e}")
                time.sleep(1)
    
    def close(self):
        """Close the producer"""
        self.producer.close()

def main():
    # Get bootstrap servers from environment
    import os
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    producer = EventProducer(bootstrap_servers)
    
    # Create threads for different event types
    threads = [
        threading.Thread(target=producer.produce_orders, daemon=True),
        threading.Thread(target=producer.produce_clicks, daemon=True),
        threading.Thread(target=producer.produce_sensor_data, daemon=True),
        threading.Thread(target=producer.produce_transactions, daemon=True)
    ]
    
    # Start all threads
    for thread in threads:
        thread.start()
    
    logger.info("All producers started. Press Ctrl+C to stop.")
    
    try:
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping producers...")
        producer.close()

if __name__ == "__main__":
    main()