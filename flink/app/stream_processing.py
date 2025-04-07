#!/usr/bin/env python3

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_kafka_source():
    """Create Kafka source for streaming data"""
    return KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("web-events") \
        .set_group_id("flink-consumer-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(JsonRowDeserializationSchema.builder()
                                     .type_info(Types.STRING()).build()) \
        .build()

def process_event(event):
    """Process individual event"""
    try:
        data = json.loads(event)
        return {
            'event_type': data.get('event_type', 'unknown'),
            'user_id': data.get('user_id', ''),
            'value': data.get('value', 0),
            'timestamp': data.get('timestamp', ''),
            'processed_at': datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error processing event: {e}")
        return None

def aggregate_events(events):
    """Aggregate events within a window"""
    if not events:
        return None
    
    # Group by event type
    event_groups = {}
    for event in events:
        if event:
            event_type = event.get('event_type', 'unknown')
            if event_type not in event_groups:
                event_groups[event_type] = []
            event_groups[event_type].append(event)
    
    # Calculate aggregations
    results = []
    for event_type, event_list in event_groups.items():
        total_count = len(event_list)
        total_value = sum(e.get('value', 0) for e in event_list)
        avg_value = total_value / total_count if total_count > 0 else 0
        
        results.append({
            'event_type': event_type,
            'count': total_count,
            'total_value': total_value,
            'avg_value': avg_value,
            'window_end': datetime.now().isoformat()
        })
    
    return results

def main():
    """Main Flink streaming application"""
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    
    # Enable checkpointing
    env.enable_checkpointing(60000)  # 60 seconds
    
    # Create Kafka source
    kafka_source = create_kafka_source()
    
    # Create data stream
    data_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "kafka-source"
    )
    
    # Process events
    processed_stream = data_stream.map(
        process_event,
        output_type=Types.PICKLED_BYTE_ARRAY()
    ).filter(lambda x: x is not None)
    
    # Window and aggregate
    windowed_stream = processed_stream \
        .window_all(TumblingProcessingTimeWindows.of(Time.seconds(30))) \
        .aggregate(
            aggregate_events,
            output_type=Types.PICKLED_BYTE_ARRAY()
        )
    
    # Print results
    windowed_stream.print()
    
    # Execute the job
    logger.info("Starting Flink streaming job...")
    env.execute("Event Processing Pipeline")

if __name__ == '__main__':
    main()