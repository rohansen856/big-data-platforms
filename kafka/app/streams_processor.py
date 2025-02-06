#!/usr/bin/env python3

import json
import time
from kafka import KafkaProducer, KafkaConsumer
import threading
import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta
import statistics

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamProcessor:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.running = True
        
        # Producer for processed results
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
    
    def order_aggregation_stream(self):
        """Stream processor for real-time order aggregations"""
        logger.info("Starting order aggregation stream processor")
        
        consumer = KafkaConsumer(
            'orders',
            bootstrap_servers=self.bootstrap_servers,
            group_id='order-aggregation-stream',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        # Windowed aggregations
        window_size = timedelta(minutes=5)
        windows = defaultdict(lambda: {
            'count': 0,
            'total_amount': 0.0,
            'products': defaultdict(int),
            'customers': set(),
            'start_time': None,
            'end_time': None
        })
        
        try:
            while self.running:
                messages = consumer.poll(timeout_ms=1000)
                
                if not messages:
                    # Check for completed windows
                    self._check_completed_windows(windows, window_size, 'order-aggregations')
                    continue
                
                for topic_partition, records in messages.items():
                    for record in records:
                        try:
                            order = record.value
                            timestamp = datetime.fromisoformat(order['timestamp'])
                            
                            # Calculate window key
                            window_start = timestamp.replace(
                                minute=(timestamp.minute // 5) * 5,
                                second=0,
                                microsecond=0
                            )
                            window_key = window_start.isoformat()
                            
                            # Update window
                            window = windows[window_key]
                            if window['start_time'] is None:
                                window['start_time'] = window_start
                                window['end_time'] = window_start + window_size
                            
                            window['count'] += 1
                            window['total_amount'] += order['total_amount']
                            window['products'][order['product_name']] += order['quantity']
                            window['customers'].add(order['customer_id'])
                            
                            logger.debug(f"Updated window {window_key}: {window['count']} orders")
                            
                        except Exception as e:
                            logger.error(f"Error processing order in stream: {e}")
                
                # Check for completed windows
                self._check_completed_windows(windows, window_size, 'order-aggregations')
                
        except Exception as e:
            logger.error(f"Stream processor error: {e}")
        finally:
            consumer.close()
    
    def click_stream_analytics(self):
        """Stream processor for real-time click analytics"""
        logger.info("Starting click stream analytics processor")
        
        consumer = KafkaConsumer(
            'user-clicks',
            bootstrap_servers=self.bootstrap_servers,
            group_id='click-analytics-stream',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        # Session tracking
        sessions = defaultdict(lambda: {
            'start_time': None,
            'last_activity': None,
            'page_views': 0,
            'unique_pages': set(),
            'actions': defaultdict(int)
        })
        
        # Page popularity (sliding window)
        page_views = defaultdict(deque)
        
        try:
            while self.running:
                messages = consumer.poll(timeout_ms=1000)
                
                if not messages:
                    continue
                
                for topic_partition, records in messages.items():
                    for record in records:
                        try:
                            click = record.value
                            timestamp = datetime.fromisoformat(click['timestamp'])
                            session_id = click['session_id']
                            
                            # Update session
                            session = sessions[session_id]
                            if session['start_time'] is None:
                                session['start_time'] = timestamp
                            
                            session['last_activity'] = timestamp
                            session['page_views'] += 1
                            session['unique_pages'].add(click['page'])
                            session['actions'][click['action']] += 1
                            
                            # Update page views (keep last hour)
                            page_views[click['page']].append(timestamp)
                            cutoff_time = timestamp - timedelta(hours=1)
                            
                            # Remove old entries
                            while (page_views[click['page']] and 
                                   page_views[click['page']][0] < cutoff_time):
                                page_views[click['page']].popleft()
                            
                            # Generate real-time insights
                            if len(sessions) % 100 == 0:
                                self._generate_click_insights(sessions, page_views)
                            
                        except Exception as e:
                            logger.error(f"Error processing click in stream: {e}")
                
                # Clean up old sessions (inactive for 30 minutes)
                self._cleanup_old_sessions(sessions, timedelta(minutes=30))
                
        except Exception as e:
            logger.error(f"Stream processor error: {e}")
        finally:
            consumer.close()
    
    def sensor_anomaly_detection(self):
        """Stream processor for IoT sensor anomaly detection"""
        logger.info("Starting sensor anomaly detection stream processor")
        
        consumer = KafkaConsumer(
            'iot-sensors',
            bootstrap_servers=self.bootstrap_servers,
            group_id='sensor-anomaly-stream',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        # Sensor baselines and moving averages
        sensor_data = defaultdict(lambda: {
            'readings': deque(maxlen=20),
            'baseline_mean': None,
            'baseline_std': None,
            'anomaly_count': 0,
            'last_anomaly': None
        })
        
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
                            timestamp = datetime.fromisoformat(reading['timestamp'])
                            
                            sensor = sensor_data[sensor_id]
                            sensor['readings'].append(value)
                            
                            # Calculate baseline after 10 readings
                            if len(sensor['readings']) >= 10:
                                readings_list = list(sensor['readings'])
                                sensor['baseline_mean'] = statistics.mean(readings_list)
                                sensor['baseline_std'] = statistics.stdev(readings_list)
                                
                                # Detect anomaly (3 standard deviations)
                                if (sensor['baseline_std'] > 0 and 
                                    abs(value - sensor['baseline_mean']) > 3 * sensor['baseline_std']):
                                    
                                    anomaly = {
                                        'sensor_id': sensor_id,
                                        'location': reading['location'],
                                        'sensor_type': reading['sensor_type'],
                                        'value': value,
                                        'baseline_mean': sensor['baseline_mean'],
                                        'baseline_std': sensor['baseline_std'],
                                        'deviation': abs(value - sensor['baseline_mean']) / sensor['baseline_std'],
                                        'timestamp': timestamp.isoformat(),
                                        'alert_type': 'statistical_anomaly'
                                    }
                                    
                                    sensor['anomaly_count'] += 1
                                    sensor['last_anomaly'] = timestamp
                                    
                                    # Send anomaly alert
                                    self.producer.send('sensor-anomalies', value=anomaly)
                                    logger.warning(f"ANOMALY DETECTED: {anomaly}")
                            
                            # Rate of change detection
                            if len(sensor['readings']) >= 2:
                                rate_of_change = abs(sensor['readings'][-1] - sensor['readings'][-2])
                                if rate_of_change > 10:  # Threshold for rapid change
                                    
                                    rapid_change = {
                                        'sensor_id': sensor_id,
                                        'location': reading['location'],
                                        'sensor_type': reading['sensor_type'],
                                        'previous_value': sensor['readings'][-2],
                                        'current_value': value,
                                        'rate_of_change': rate_of_change,
                                        'timestamp': timestamp.isoformat(),
                                        'alert_type': 'rapid_change'
                                    }
                                    
                                    self.producer.send('sensor-anomalies', value=rapid_change)
                                    logger.warning(f"RAPID CHANGE DETECTED: {rapid_change}")
                            
                        except Exception as e:
                            logger.error(f"Error processing sensor data in stream: {e}")
                
        except Exception as e:
            logger.error(f"Stream processor error: {e}")
        finally:
            consumer.close()
    
    def transaction_risk_scoring(self):
        """Stream processor for real-time transaction risk scoring"""
        logger.info("Starting transaction risk scoring stream processor")
        
        consumer = KafkaConsumer(
            'transactions',
            bootstrap_servers=self.bootstrap_servers,
            group_id='transaction-risk-stream',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        # Account profiles
        account_profiles = defaultdict(lambda: {
            'transaction_history': deque(maxlen=50),
            'avg_amount': 0.0,
            'total_transactions': 0,
            'locations': set(),
            'merchants': set(),
            'risk_score': 0.0
        })
        
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
                            
                            profile = account_profiles[account_id]
                            profile['transaction_history'].append(transaction)
                            profile['total_transactions'] += 1
                            profile['locations'].add(transaction['location'])
                            
                            if transaction['merchant']:
                                profile['merchants'].add(transaction['merchant'])
                            
                            # Calculate average amount
                            amounts = [t['amount'] for t in profile['transaction_history']]
                            profile['avg_amount'] = sum(amounts) / len(amounts)
                            
                            # Risk scoring
                            risk_score = 0.0
                            risk_factors = []
                            
                            # Amount-based risk
                            if amount > profile['avg_amount'] * 3:
                                risk_score += 30
                                risk_factors.append('unusual_amount')
                            
                            # Location-based risk
                            if len(profile['locations']) > 5:
                                risk_score += 20
                                risk_factors.append('multiple_locations')
                            
                            # Frequency-based risk
                            recent_transactions = [
                                t for t in profile['transaction_history'][-10:]
                                if datetime.fromisoformat(t['timestamp']) > 
                                datetime.fromisoformat(transaction['timestamp']) - timedelta(hours=1)
                            ]
                            
                            if len(recent_transactions) > 5:
                                risk_score += 25
                                risk_factors.append('high_frequency')
                            
                            # Failed transaction risk
                            if transaction['status'] == 'failed':
                                risk_score += 15
                                risk_factors.append('failed_transaction')
                            
                            profile['risk_score'] = risk_score
                            
                            # Generate risk alert for high-risk transactions
                            if risk_score > 50:
                                risk_alert = {
                                    'transaction_id': transaction['transaction_id'],
                                    'account_id': account_id,
                                    'amount': amount,
                                    'risk_score': risk_score,
                                    'risk_factors': risk_factors,
                                    'timestamp': transaction['timestamp'],
                                    'alert_type': 'high_risk_transaction'
                                }
                                
                                self.producer.send('transaction-risk-alerts', value=risk_alert)
                                logger.warning(f"HIGH RISK TRANSACTION: {risk_alert}")
                            
                        except Exception as e:
                            logger.error(f"Error processing transaction in stream: {e}")
                
        except Exception as e:
            logger.error(f"Stream processor error: {e}")
        finally:
            consumer.close()
    
    def _check_completed_windows(self, windows, window_size, output_topic):
        """Check for completed windows and emit results"""
        current_time = datetime.now()
        completed_windows = []
        
        for window_key, window in windows.items():
            if (window['end_time'] and 
                current_time > window['end_time'] + timedelta(seconds=30)):
                completed_windows.append(window_key)
        
        for window_key in completed_windows:
            window = windows[window_key]
            
            # Convert sets to lists for JSON serialization
            result = {
                'window_start': window['start_time'].isoformat(),
                'window_end': window['end_time'].isoformat(),
                'order_count': window['count'],
                'total_amount': window['total_amount'],
                'avg_amount': window['total_amount'] / window['count'] if window['count'] > 0 else 0,
                'unique_customers': len(window['customers']),
                'top_products': dict(sorted(window['products'].items(), 
                                          key=lambda x: x[1], reverse=True)[:5]),
                'timestamp': current_time.isoformat()
            }
            
            self.producer.send(output_topic, value=result)
            logger.info(f"Emitted window result: {result}")
            
            del windows[window_key]
    
    def _generate_click_insights(self, sessions, page_views):
        """Generate real-time insights from click data"""
        try:
            # Calculate metrics
            total_sessions = len(sessions)
            avg_session_duration = 0
            active_sessions = 0
            
            current_time = datetime.now()
            
            for session in sessions.values():
                if session['start_time'] and session['last_activity']:
                    duration = (session['last_activity'] - session['start_time']).total_seconds()
                    avg_session_duration += duration
                    
                    if (current_time - session['last_activity']).total_seconds() < 300:  # 5 minutes
                        active_sessions += 1
            
            avg_session_duration = avg_session_duration / total_sessions if total_sessions > 0 else 0
            
            # Top pages in last hour
            top_pages = []
            for page, timestamps in page_views.items():
                top_pages.append((page, len(timestamps)))
            
            top_pages.sort(key=lambda x: x[1], reverse=True)
            
            insight = {
                'total_sessions': total_sessions,
                'active_sessions': active_sessions,
                'avg_session_duration_seconds': avg_session_duration,
                'top_pages_last_hour': dict(top_pages[:5]),
                'timestamp': current_time.isoformat()
            }
            
            self.producer.send('click-insights', value=insight)
            logger.info(f"Click insights: {insight}")
            
        except Exception as e:
            logger.error(f"Error generating click insights: {e}")
    
    def _cleanup_old_sessions(self, sessions, timeout):
        """Clean up old inactive sessions"""
        current_time = datetime.now()
        old_sessions = []
        
        for session_id, session in sessions.items():
            if (session['last_activity'] and 
                current_time - session['last_activity'] > timeout):
                old_sessions.append(session_id)
        
        for session_id in old_sessions:
            del sessions[session_id]
    
    def close(self):
        """Close the stream processor"""
        self.running = False
        self.producer.close()

def main():
    import os
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    processor = StreamProcessor(bootstrap_servers)
    
    # Create threads for different stream processors
    threads = [
        threading.Thread(target=processor.order_aggregation_stream, daemon=True),
        threading.Thread(target=processor.click_stream_analytics, daemon=True),
        threading.Thread(target=processor.sensor_anomaly_detection, daemon=True),
        threading.Thread(target=processor.transaction_risk_scoring, daemon=True)
    ]
    
    # Start all threads
    for thread in threads:
        thread.start()
    
    logger.info("All stream processors started. Press Ctrl+C to stop.")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping stream processors...")
        processor.close()

if __name__ == "__main__":
    main()