#!/usr/bin/env python3

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import logging
from datetime import datetime
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ParseJsonDoFn(beam.DoFn):
    """Parse JSON records and extract fields"""
    def process(self, element):
        try:
            record = json.loads(element)
            yield {
                'timestamp': record.get('timestamp', ''),
                'event_type': record.get('event_type', ''),
                'user_id': record.get('user_id', ''),
                'value': record.get('value', 0)
            }
        except Exception as e:
            logger.error(f"Error parsing JSON: {e}")

class FilterValidRecords(beam.DoFn):
    """Filter out invalid records"""
    def process(self, element):
        if element['timestamp'] and element['event_type']:
            yield element

class AggregateByEventType(beam.DoFn):
    """Aggregate records by event type"""
    def process(self, element):
        event_type, records = element
        total_count = len(records)
        total_value = sum(record['value'] for record in records)
        
        yield {
            'event_type': event_type,
            'count': total_count,
            'total_value': total_value,
            'avg_value': total_value / total_count if total_count > 0 else 0,
            'processed_at': datetime.now().isoformat()
        }

def run_pipeline():
    """Main pipeline function"""
    pipeline_options = PipelineOptions([
        '--runner=DirectRunner',
        '--project=beam-demo',
        '--temp_location=/tmp/beam-temp',
        '--staging_location=/tmp/beam-staging'
    ])
    
    input_file = '/shared-data/sample_data.json'
    output_file = '/shared-data/beam_output'
    
    # Create sample data if it doesn't exist
    if not os.path.exists(input_file):
        create_sample_data(input_file)
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        logger.info("Starting Beam pipeline...")
        
        # Read data
        raw_data = (
            pipeline
            | 'ReadFromFile' >> beam.io.ReadFromText(input_file)
        )
        
        # Process data
        processed_data = (
            raw_data
            | 'ParseJSON' >> beam.ParDo(ParseJsonDoFn())
            | 'FilterValid' >> beam.ParDo(FilterValidRecords())
            | 'GroupByEventType' >> beam.GroupBy(lambda x: x['event_type'])
            | 'AggregateData' >> beam.ParDo(AggregateByEventType())
        )
        
        # Write results
        (
            processed_data
            | 'FormatOutput' >> beam.Map(lambda x: json.dumps(x))
            | 'WriteToFile' >> beam.io.WriteToText(output_file, file_name_suffix='.json')
        )
        
        logger.info("Pipeline completed successfully!")

def create_sample_data(filename):
    """Create sample data for testing"""
    import random
    from faker import Faker
    
    fake = Faker()
    sample_data = []
    
    event_types = ['page_view', 'click', 'purchase', 'signup', 'login']
    
    for _ in range(1000):
        record = {
            'timestamp': fake.date_time_this_month().isoformat(),
            'event_type': random.choice(event_types),
            'user_id': fake.uuid4(),
            'value': random.randint(1, 100)
        }
        sample_data.append(json.dumps(record))
    
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as f:
        f.write('\n'.join(sample_data))
    
    logger.info(f"Created sample data: {filename}")

if __name__ == '__main__':
    run_pipeline()