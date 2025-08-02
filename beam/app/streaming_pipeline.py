#!/usr/bin/env python3

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
import json
import logging
from datetime import datetime
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProcessKafkaMessage(beam.DoFn):
    """Process Kafka messages"""
    def process(self, element):
        try:
            # element is a tuple (key, value)
            key, value = element
            
            # Parse JSON message
            message = json.loads(value.decode('utf-8'))
            
            # Add processing timestamp
            message['processed_at'] = datetime.now().isoformat()
            
            # Transform message
            transformed = {
                'original_key': key.decode('utf-8') if key else None,
                'event_type': message.get('event_type', 'unknown'),
                'user_id': message.get('user_id', ''),
                'value': message.get('value', 0),
                'timestamp': message.get('timestamp', ''),
                'processed_at': message['processed_at']
            }
            
            yield transformed
            
        except Exception as e:
            logger.error(f"Error processing Kafka message: {e}")
            # Yield error record for monitoring
            yield {
                'error': str(e),
                'raw_message': str(element),
                'processed_at': datetime.now().isoformat()
            }

class WindowedAggregation(beam.DoFn):
    """Aggregate data within windows"""
    def process(self, element):
        event_type, records = element
        
        # Calculate aggregations
        count = len(records)
        total_value = sum(record.get('value', 0) for record in records)
        avg_value = total_value / count if count > 0 else 0
        
        result = {
            'event_type': event_type,
            'window_count': count,
            'total_value': total_value,
            'avg_value': avg_value,
            'aggregated_at': datetime.now().isoformat()
        }
        
        yield result

def run_streaming_pipeline():
    """Main streaming pipeline function"""
    pipeline_options = PipelineOptions([
        '--runner=DirectRunner',
        '--streaming',
        '--project=beam-streaming-demo',
        '--temp_location=/tmp/beam-temp',
        '--staging_location=/tmp/beam-staging'
    ])
    
    kafka_config = {
        'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
        'topics': ['web-events'],
        'group_id': 'beam-consumer-group'
    }
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        logger.info("Starting Beam streaming pipeline...")
        
        # Read from Kafka
        kafka_messages = (
            pipeline
            | 'ReadFromKafka' >> ReadFromKafka(
                consumer_config={
                    'bootstrap.servers': kafka_config['bootstrap_servers'],
                    'group.id': kafka_config['group_id'],
                    'auto.offset.reset': 'latest'
                },
                topics=kafka_config['topics']
            )
        )
        
        # Process messages
        processed_messages = (
            kafka_messages
            | 'ProcessMessages' >> beam.ParDo(ProcessKafkaMessage())
            | 'WindowIntoFixedWindows' >> beam.WindowInto(
                beam.window.FixedWindows(60)  # 60 second windows
            )
            | 'GroupByEventType' >> beam.GroupBy(lambda x: x.get('event_type', 'unknown'))
            | 'AggregateInWindow' >> beam.ParDo(WindowedAggregation())
        )
        
        # Write results back to Kafka
        (
            processed_messages
            | 'FormatForKafka' >> beam.Map(
                lambda x: (x['event_type'].encode('utf-8'), json.dumps(x).encode('utf-8'))
            )
            | 'WriteToKafka' >> WriteToKafka(
                producer_config={
                    'bootstrap.servers': kafka_config['bootstrap_servers']
                },
                topic='beam-aggregated-events'
            )
        )
        
        # Also write to file for monitoring
        (
            processed_messages
            | 'FormatForFile' >> beam.Map(lambda x: json.dumps(x))
            | 'WriteToFile' >> beam.io.WriteToText(
                '/shared-data/beam_streaming_output',
                file_name_suffix='.json'
            )
        )
        
        logger.info("Streaming pipeline setup completed!")

if __name__ == '__main__':
    run_streaming_pipeline()