#!/usr/bin/env python3

import json
import time
from kafka import KafkaConsumer, TopicPartition
import threading
import logging
from collections import defaultdict, deque
import signal
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EventConsumer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.running = True
        self.stats = defaultdict(lambda: {'count': 0, 'errors': 0})
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        logger.info("Received termination signal, shutting down...")
        self.running = False
    
    def consume_orders(self, group_id='order-processors'):
        """Consume and process order events"""
        logger.info(f"Starting order consumer for group: {group_id}")
        
        consumer = KafkaConsumer(
            'orders',
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            max_poll_records=100
        )
        
        order_totals = defaultdict(float)
        
        try:
            while self.running:
                messages = consumer.poll(timeout_ms=1000)
                
                if not messages:
                    continue
                
                for topic_partition, records in messages.items():
                    for record in records:
                        try:
                            order = record.value
                            
                            # Process order
                            customer_id = order['customer_id']
                            amount = order['total_amount']
                            
                            order_totals[customer_id] += amount
                            
                            # Log processing
                            logger.info(f"Processed order {order['order_id']}: "
                                      f"Customer {customer_id[:8]}... - ${amount:.2f}")
                            
                            # Simulate order processing
                            if order['order_status'] == 'pending':
                                logger.info(f"Order {order['order_id']} confirmed")
                            
                            self.stats['orders']['count'] += 1
                            
                        except Exception as e:
                            logger.error(f"Error processing order: {e}")
                            self.stats['orders']['errors'] += 1
                
                # Commit offsets
                consumer.commit()
                
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            consumer.close()
            logger.info(f"Order consumer stopped. Processed {self.stats['orders']['count']} orders")
    
    def consume_clicks(self, group_id='analytics-processors'):
        """Consume and process click events"""
        logger.info(f"Starting click consumer for group: {group_id}")
        
        consumer = KafkaConsumer(
            'user-clicks',
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            max_poll_records=500
        )
        
        # Analytics data
        page_views = defaultdict(int)
        user_sessions = defaultdict(set)
        recent_clicks = deque(maxlen=1000)
        
        try:
            while self.running:
                messages = consumer.poll(timeout_ms=1000)
                
                if not messages:
                    continue
                
                for topic_partition, records in messages.items():
                    for record in records:
                        try:
                            click = record.value
                            
                            # Update analytics
                            page_views[click['page']] += 1
                            user_sessions[click['user_id']].add(click['session_id'])
                            recent_clicks.append(click)
                            
                            # Real-time analytics
                            if len(recent_clicks) >= 100:
                                top_pages = sorted(page_views.items(), 
                                                 key=lambda x: x[1], reverse=True)[:5]
                                logger.info(f"Top pages: {dict(top_pages)}")
                                logger.info(f"Active users: {len(user_sessions)}")
                            
                            self.stats['clicks']['count'] += 1
                            
                        except Exception as e:
                            logger.error(f"Error processing click: {e}")
                            self.stats['clicks']['errors'] += 1
                
                consumer.commit()
                
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            consumer.close()
            logger.info(f"Click consumer stopped. Processed {self.stats['clicks']['count']} clicks")
    
    def consume_sensor_data(self, group_id='iot-processors'):
        """Consume and process sensor data"""
        logger.info(f"Starting sensor consumer for group: {group_id}")
        
        consumer = KafkaConsumer(
            'iot-sensors',
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        # Sensor monitoring
        sensor_readings = defaultdict(deque)
        alerts = []
        
        try:
            while self.running:
                messages = consumer.poll(timeout_ms=1000)
                
                if not messages:
                    continue
                
                for topic_partition, records in messages.items():
                    for record in records:
                        try:
                            reading = record.value
                            
                            sensor_id = reading['sensor_id']
                            value = reading['value']
                            
                            # Store reading
                            sensor_readings[sensor_id].append(value)
                            if len(sensor_readings[sensor_id]) > 10:
                                sensor_readings[sensor_id].popleft()
                            
                            # Check for alerts
                            if reading['alert']:
                                alert_msg = (f"ALERT: {sensor_id} at {reading['location']} "
                                           f"- {reading['sensor_type']}: {value} {reading['unit']}")
                                logger.warning(alert_msg)
                                alerts.append(alert_msg)
                            
                            # Calculate moving average
                            if len(sensor_readings[sensor_id]) >= 3:
                                avg = sum(sensor_readings[sensor_id]) / len(sensor_readings[sensor_id])
                                logger.info(f"Sensor {sensor_id}: {value} {reading['unit']} "
                                          f"(avg: {avg:.2f})")
                            
                            self.stats['sensors']['count'] += 1
                            
                        except Exception as e:
                            logger.error(f"Error processing sensor data: {e}")
                            self.stats['sensors']['errors'] += 1
                
                consumer.commit()
                
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            consumer.close()
            logger.info(f"Sensor consumer stopped. Processed {self.stats['sensors']['count']} readings")
            if alerts:
                logger.warning(f"Total alerts: {len(alerts)}")
    
    def consume_transactions(self, group_id='fraud-detection'):
        """Consume and process transaction events"""
        logger.info(f"Starting transaction consumer for group: {group_id}")
        
        consumer = KafkaConsumer(
            'transactions',
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        # Fraud detection
        account_activity = defaultdict(list)
        suspicious_transactions = []
        
        try:
            while self.running:
                messages = consumer.poll(timeout_ms=1000)
                
                if not messages:
                    continue
                
                for topic_partition, records in messages.items():
                    for record in records:
                        try:
                            transaction = record.value
                            
                            account_id = transaction['account_id']
                            amount = transaction['amount']
                            
                            # Track account activity
                            account_activity[account_id].append(amount)
                            if len(account_activity[account_id]) > 10:
                                account_activity[account_id].pop(0)
                            
                            # Simple fraud detection
                            suspicious = False
                            
                            # Large amount
                            if amount > 3000:
                                suspicious = True
                                reason = "Large amount"
                            
                            # High frequency
                            elif len(account_activity[account_id]) > 5:
                                recent_sum = sum(account_activity[account_id][-5:])
                                if recent_sum > 2000:
                                    suspicious = True
                                    reason = "High frequency"
                            
                            if suspicious:
                                fraud_alert = {
                                    'transaction_id': transaction['transaction_id'],
                                    'account_id': account_id,
                                    'amount': amount,
                                    'reason': reason,
                                    'timestamp': transaction['timestamp']
                                }
                                suspicious_transactions.append(fraud_alert)
                                logger.warning(f"FRAUD ALERT: {fraud_alert}")
                            
                            logger.info(f"Processed transaction {transaction['transaction_id']}: "
                                      f"${amount:.2f} - {transaction['type']}")
                            
                            self.stats['transactions']['count'] += 1
                            
                        except Exception as e:
                            logger.error(f"Error processing transaction: {e}")
                            self.stats['transactions']['errors'] += 1
                
                consumer.commit()
                
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            consumer.close()
            logger.info(f"Transaction consumer stopped. Processed {self.stats['transactions']['count']} transactions")
            logger.info(f"Fraud alerts: {len(suspicious_transactions)}")
    
    def print_stats(self):
        """Print consumer statistics"""
        while self.running:
            time.sleep(30)
            logger.info("=== Consumer Statistics ===")
            for topic, stats in self.stats.items():
                logger.info(f"{topic}: {stats['count']} processed, {stats['errors']} errors")

def main():
    # Get bootstrap servers from environment
    import os
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    consumer = EventConsumer(bootstrap_servers)
    
    # Create threads for different consumer groups
    threads = [
        threading.Thread(target=consumer.consume_orders, daemon=True),
        threading.Thread(target=consumer.consume_clicks, daemon=True),
        threading.Thread(target=consumer.consume_sensor_data, daemon=True),
        threading.Thread(target=consumer.consume_transactions, daemon=True),
        threading.Thread(target=consumer.print_stats, daemon=True)
    ]
    
    # Start all threads
    for thread in threads:
        thread.start()
    
    logger.info("All consumers started. Press Ctrl+C to stop.")
    
    try:
        # Keep main thread alive
        while consumer.running:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping consumers...")
        consumer.running = False
    
    # Wait for threads to complete
    for thread in threads:
        thread.join(timeout=5)
    
    logger.info("All consumers stopped.")

if __name__ == "__main__":
    main()