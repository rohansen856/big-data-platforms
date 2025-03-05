#!/usr/bin/env python3

import time
import json
import random
import threading
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
from clickhouse_driver import Client
from faker import Faker
import pandas as pd
import numpy as np
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

class UnifiedDataGenerator:
    def __init__(self):
        self.kafka_producer = None
        self.clickhouse_client = None
        self.running = True
        
        # Initialize connections
        self.init_connections()
        
    def init_connections(self):
        """Initialize connections to data systems"""
        # Kafka connection
        try:
            kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            logger.info("Connected to Kafka")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
        
        # ClickHouse connection
        try:
            ch_host = os.getenv('CLICKHOUSE_HOST', 'localhost')
            ch_port = int(os.getenv('CLICKHOUSE_PORT', '9000'))
            self.clickhouse_client = Client(host=ch_host, port=ch_port)
            logger.info("Connected to ClickHouse")
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
    
    def generate_and_send_data(self):
        """Generate data and send to both Kafka and ClickHouse"""
        logger.info("Starting unified data generation")
        
        while self.running:
            try:
                # Generate different types of data
                events = self.generate_events(50)
                transactions = self.generate_transactions(20)
                sensor_data = self.generate_sensor_readings(30)
                
                # Send to Kafka
                if self.kafka_producer:
                    self.send_to_kafka(events, 'events')
                    self.send_to_kafka(transactions, 'transactions')
                    self.send_to_kafka(sensor_data, 'sensor-data')
                
                # Send to ClickHouse
                if self.clickhouse_client:
                    self.send_to_clickhouse(events, 'analytics.events')
                    self.send_to_clickhouse(transactions, 'analytics.transactions')
                    self.send_to_clickhouse(sensor_data, 'analytics.sensor_data')
                
                # Save to shared data directory for other services
                self.save_to_shared_data(events, transactions, sensor_data)
                
                logger.info(f"Generated and sent {len(events)} events, {len(transactions)} transactions, {len(sensor_data)} sensor readings")
                
                time.sleep(30)  # Generate data every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in data generation: {e}")
                time.sleep(10)
    
    def generate_events(self, count):
        """Generate web/app events"""
        events = []
        event_types = ['page_view', 'click', 'purchase', 'login', 'search']
        
        for _ in range(count):
            event = {
                'timestamp': datetime.now().isoformat(),
                'user_id': fake.uuid4(),
                'session_id': fake.uuid4(),
                'event_type': random.choice(event_types),
                'page_url': f"/{fake.word()}",
                'user_agent': fake.user_agent(),
                'ip_address': fake.ipv4(),
                'country': fake.country_code(),
                'device_type': random.choice(['desktop', 'mobile', 'tablet']),
                'value': round(random.uniform(0, 100), 2)
            }
            events.append(event)
        
        return events
    
    def generate_transactions(self, count):
        """Generate financial transactions"""
        transactions = []
        
        for _ in range(count):
            transaction = {
                'timestamp': datetime.now().isoformat(),
                'transaction_id': fake.uuid4(),
                'user_id': fake.uuid4(),
                'amount': round(random.uniform(10, 1000), 2),
                'currency': random.choice(['USD', 'EUR', 'GBP']),
                'merchant_id': fake.uuid4(),
                'merchant_category': random.choice(['grocery', 'gas', 'restaurant']),
                'status': random.choice(['completed', 'pending', 'failed']),
                'location': fake.city()
            }
            transactions.append(transaction)
        
        return transactions
    
    def generate_sensor_readings(self, count):
        """Generate IoT sensor readings"""
        readings = []
        sensors = ['TEMP-001', 'HUM-001', 'PRESS-001', 'VIBR-001']
        
        for _ in range(count):
            sensor_id = random.choice(sensors)
            sensor_type = sensor_id.split('-')[0].lower()
            
            if sensor_type == 'temp':
                value = round(random.uniform(18, 30), 2)
                unit = '°C'
            elif sensor_type == 'hum':
                value = round(random.uniform(40, 70), 2)
                unit = '%'
            elif sensor_type == 'press':
                value = round(random.uniform(1000, 1030), 2)
                unit = 'hPa'
            else:
                value = round(random.uniform(0, 10), 2)
                unit = 'm/s²'
            
            reading = {
                'timestamp': datetime.now().isoformat(),
                'sensor_id': sensor_id,
                'sensor_type': sensor_type,
                'location': f'Building-{random.randint(1, 5)}',
                'value': value,
                'unit': unit,
                'status': random.choice(['normal', 'warning', 'critical'])
            }
            readings.append(reading)
        
        return readings
    
    def send_to_kafka(self, data, topic):
        """Send data to Kafka topic"""
        try:
            for record in data:
                self.kafka_producer.send(topic, value=record)
            self.kafka_producer.flush()
        except Exception as e:
            logger.error(f"Error sending to Kafka topic {topic}: {e}")
    
    def send_to_clickhouse(self, data, table):
        """Send data to ClickHouse table"""
        try:
            if table == 'analytics.events':
                formatted_data = []
                for event in data:
                    formatted_data.append({
                        'timestamp': datetime.fromisoformat(event['timestamp']),
                        'user_id': event['user_id'],
                        'session_id': event['session_id'],
                        'event_type': event['event_type'],
                        'page_url': event['page_url'],
                        'referrer': '',
                        'user_agent': event['user_agent'],
                        'ip_address': event['ip_address'],
                        'country': event['country'],
                        'device_type': event['device_type'],
                        'browser': 'Chrome',
                        'os': 'Windows',
                        'screen_width': 1920,
                        'screen_height': 1080,
                        'duration': random.randint(1, 300),
                        'value': event['value']
                    })
                
                self.clickhouse_client.execute(f'INSERT INTO {table} VALUES', formatted_data)
            
            elif table == 'analytics.transactions':
                formatted_data = []
                for txn in data:
                    formatted_data.append({
                        'timestamp': datetime.fromisoformat(txn['timestamp']),
                        'transaction_id': txn['transaction_id'],
                        'user_id': txn['user_id'],
                        'account_id': fake.uuid4(),
                        'transaction_type': 'purchase',
                        'amount': txn['amount'],
                        'currency': txn['currency'],
                        'merchant_id': txn['merchant_id'],
                        'merchant_category': txn['merchant_category'],
                        'payment_method': 'credit_card',
                        'location': txn['location'],
                        'latitude': round(random.uniform(-90, 90), 6),
                        'longitude': round(random.uniform(-180, 180), 6),
                        'status': txn['status'],
                        'risk_score': round(random.uniform(0, 1), 3),
                        'fraud_flag': 0
                    })
                
                self.clickhouse_client.execute(f'INSERT INTO {table} VALUES', formatted_data)
            
            elif table == 'analytics.sensor_data':
                formatted_data = []
                for reading in data:
                    formatted_data.append({
                        'timestamp': datetime.fromisoformat(reading['timestamp']),
                        'sensor_id': reading['sensor_id'],
                        'sensor_type': reading['sensor_type'],
                        'location': reading['location'],
                        'value': reading['value'],
                        'unit': reading['unit'],
                        'status': reading['status'],
                        'battery_level': round(random.uniform(20, 100), 1),
                        'signal_strength': random.randint(1, 5),
                        'temperature': round(random.uniform(20, 30), 1),
                        'humidity': round(random.uniform(40, 60), 1),
                        'metadata': '{}'
                    })
                
                self.clickhouse_client.execute(f'INSERT INTO {table} VALUES', formatted_data)
                
        except Exception as e:
            logger.error(f"Error sending to ClickHouse table {table}: {e}")
    
    def save_to_shared_data(self, events, transactions, sensor_data):
        """Save data to shared directory for other services"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Save as CSV files
            events_df = pd.DataFrame(events)
            transactions_df = pd.DataFrame(transactions)
            sensor_df = pd.DataFrame(sensor_data)
            
            events_df.to_csv(f'/shared-data/events_{timestamp}.csv', index=False)
            transactions_df.to_csv(f'/shared-data/transactions_{timestamp}.csv', index=False)
            sensor_df.to_csv(f'/shared-data/sensor_data_{timestamp}.csv', index=False)
            
            # Save as JSON files
            with open(f'/shared-data/events_{timestamp}.json', 'w') as f:
                json.dump(events, f, indent=2)
            
            with open(f'/shared-data/transactions_{timestamp}.json', 'w') as f:
                json.dump(transactions, f, indent=2)
            
            with open(f'/shared-data/sensor_data_{timestamp}.json', 'w') as f:
                json.dump(sensor_data, f, indent=2)
            
            # Create latest symlinks
            os.makedirs('/shared-data/latest', exist_ok=True)
            
            # Update latest files
            events_df.to_csv('/shared-data/latest/events.csv', index=False)
            transactions_df.to_csv('/shared-data/latest/transactions.csv', index=False)
            sensor_df.to_csv('/shared-data/latest/sensor_data.csv', index=False)
            
        except Exception as e:
            logger.error(f"Error saving to shared data: {e}")
    
    def generate_historical_data(self):
        """Generate historical data for analytics"""
        logger.info("Generating historical data...")
        
        # Generate data for the past 7 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        historical_events = []
        historical_transactions = []
        historical_sensors = []
        
        # Generate data for each hour in the past week
        current_date = start_date
        while current_date < end_date:
            # Generate events for this hour
            for _ in range(random.randint(50, 200)):
                event = {
                    'timestamp': current_date.isoformat(),
                    'user_id': fake.uuid4(),
                    'session_id': fake.uuid4(),
                    'event_type': random.choice(['page_view', 'click', 'purchase', 'login', 'search']),
                    'page_url': f"/{fake.word()}",
                    'user_agent': fake.user_agent(),
                    'ip_address': fake.ipv4(),
                    'country': fake.country_code(),
                    'device_type': random.choice(['desktop', 'mobile', 'tablet']),
                    'value': round(random.uniform(0, 100), 2)
                }
                historical_events.append(event)
            
            # Generate transactions for this hour
            for _ in range(random.randint(20, 80)):
                transaction = {
                    'timestamp': current_date.isoformat(),
                    'transaction_id': fake.uuid4(),
                    'user_id': fake.uuid4(),
                    'amount': round(random.uniform(10, 1000), 2),
                    'currency': random.choice(['USD', 'EUR', 'GBP']),
                    'merchant_id': fake.uuid4(),
                    'merchant_category': random.choice(['grocery', 'gas', 'restaurant']),
                    'status': random.choice(['completed', 'pending', 'failed']),
                    'location': fake.city()
                }
                historical_transactions.append(transaction)
            
            # Generate sensor data for this hour
            for _ in range(random.randint(30, 60)):
                sensor_id = random.choice(['TEMP-001', 'HUM-001', 'PRESS-001', 'VIBR-001'])
                sensor_type = sensor_id.split('-')[0].lower()
                
                if sensor_type == 'temp':
                    value = round(random.uniform(18, 30), 2)
                    unit = '°C'
                elif sensor_type == 'hum':
                    value = round(random.uniform(40, 70), 2)
                    unit = '%'
                elif sensor_type == 'press':
                    value = round(random.uniform(1000, 1030), 2)
                    unit = 'hPa'
                else:
                    value = round(random.uniform(0, 10), 2)
                    unit = 'm/s²'
                
                reading = {
                    'timestamp': current_date.isoformat(),
                    'sensor_id': sensor_id,
                    'sensor_type': sensor_type,
                    'location': f'Building-{random.randint(1, 5)}',
                    'value': value,
                    'unit': unit,
                    'status': random.choice(['normal', 'warning', 'critical'])
                }
                historical_sensors.append(reading)
            
            current_date += timedelta(hours=1)
        
        # Save historical data
        logger.info(f"Saving {len(historical_events)} historical events")
        logger.info(f"Saving {len(historical_transactions)} historical transactions")
        logger.info(f"Saving {len(historical_sensors)} historical sensor readings")
        
        # Send to ClickHouse in batches
        if self.clickhouse_client:
            batch_size = 1000
            
            # Send events
            for i in range(0, len(historical_events), batch_size):
                batch = historical_events[i:i+batch_size]
                self.send_to_clickhouse(batch, 'analytics.events')
                time.sleep(1)
            
            # Send transactions
            for i in range(0, len(historical_transactions), batch_size):
                batch = historical_transactions[i:i+batch_size]
                self.send_to_clickhouse(batch, 'analytics.transactions')
                time.sleep(1)
            
            # Send sensor data
            for i in range(0, len(historical_sensors), batch_size):
                batch = historical_sensors[i:i+batch_size]
                self.send_to_clickhouse(batch, 'analytics.sensor_data')
                time.sleep(1)
        
        logger.info("Historical data generation completed")
    
    def stop(self):
        """Stop data generation"""
        self.running = False
        if self.kafka_producer:
            self.kafka_producer.close()

def main():
    logger.info("Starting unified data generator")
    
    # Wait for services to be ready
    time.sleep(60)
    
    try:
        generator = UnifiedDataGenerator()
        
        # Generate historical data first
        generator.generate_historical_data()
        
        # Start real-time data generation
        generator.generate_and_send_data()
        
    except KeyboardInterrupt:
        logger.info("Stopping data generator...")
        generator.stop()
    except Exception as e:
        logger.error(f"Error in data generator: {e}")

if __name__ == "__main__":
    main()