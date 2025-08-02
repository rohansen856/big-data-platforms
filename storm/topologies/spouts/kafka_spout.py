#!/usr/bin/env python3

from streamparse import Spout
from kafka import KafkaConsumer
import json
import logging
import os

logger = logging.getLogger(__name__)

class KafkaSpout(Spout):
    """Kafka spout for reading messages from Kafka topics"""
    
    def initialize(self, stormconf, context):
        """Initialize the spout"""
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        self.topic = os.getenv('KAFKA_INPUT_TOPIC', 'web-events')
        self.group_id = os.getenv('KAFKA_GROUP_ID', 'storm-consumer-group')
        
        # Create Kafka consumer
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_servers,
            group_id=self.group_id,
            value_deserializer=lambda m: m.decode('utf-8'),
            auto_offset_reset='latest'
        )
        
        logger.info(f"Initialized Kafka spout for topic: {self.topic}")
        
    def next_tuple(self):
        """Get the next tuple from Kafka"""
        try:
            # Poll for messages with timeout
            message_batch = self.consumer.poll(timeout_ms=100)
            
            for topic_partition, messages in message_batch.items():
                for message in messages:
                    # Emit the message value
                    self.emit([message.value])
                    
        except Exception as e:
            logger.error(f"Error reading from Kafka: {e}")
            
    def ack(self, tup_id):
        """Acknowledge successful processing"""
        pass
        
    def fail(self, tup_id):
        """Handle failed processing"""
        logger.warning(f"Failed to process tuple: {tup_id}")