#!/usr/bin/env python3

import json
import logging
import os
from datetime import datetime
from faker import Faker
import random

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

def create_sample_data():
    """Create sample data for testing"""
    sample_data = []
    event_types = ['page_view', 'click', 'purchase', 'signup', 'login']
    
    for _ in range(1000):
        record = {
            'timestamp': fake.date_time_this_month().isoformat(),
            'event_type': random.choice(event_types),
            'user_id': fake.uuid4(),
            'value': random.randint(1, 100)
        }
        sample_data.append(record)
    
    return sample_data

def process_data(data):
    """Process the data and create aggregations"""
    logger.info(f"Processing {len(data)} records...")
    
    # Group by event type
    event_groups = {}
    for record in data:
        event_type = record['event_type']
        if event_type not in event_groups:
            event_groups[event_type] = []
        event_groups[event_type].append(record)
    
    # Calculate aggregations
    results = []
    for event_type, records in event_groups.items():
        total_count = len(records)
        total_value = sum(record['value'] for record in records)
        avg_value = total_value / total_count if total_count > 0 else 0
        
        result = {
            'event_type': event_type,
            'count': total_count,
            'total_value': total_value,
            'avg_value': avg_value,
            'processed_at': datetime.now().isoformat()
        }
        results.append(result)
    
    return results

def save_results(results, filename):
    """Save results to file"""
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    with open(filename, 'w') as f:
        for result in results:
            f.write(json.dumps(result) + '\n')
    
    logger.info(f"Results saved to {filename}")

def main():
    """Main processing function"""
    logger.info("Starting simple batch processing...")
    
    # Create sample data
    data = create_sample_data()
    
    # Process data
    results = process_data(data)
    
    # Save results
    output_file = '/shared-data/beam_simple_batch_output.json'
    save_results(results, output_file)
    
    logger.info("Batch processing completed successfully!")
    
    # Print summary
    for result in results:
        print(f"Event Type: {result['event_type']}, Count: {result['count']}, Avg Value: {result['avg_value']:.2f}")

if __name__ == '__main__':
    main()