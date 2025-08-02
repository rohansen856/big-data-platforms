#!/usr/bin/env python3

import csv
import random
import os
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

def generate_sample_data(filename, num_records=10000):
    """Generate sample CSV data for Pig analysis"""
    
    event_types = ['page_view', 'click', 'purchase', 'signup', 'login', 'logout', 'search']
    pages = [
        '/home', '/products', '/about', '/contact', '/login', '/signup',
        '/product/1', '/product/2', '/product/3', '/cart', '/checkout',
        '/profile', '/settings', '/help', '/blog', '/news'
    ]
    countries = ['US', 'UK', 'CA', 'DE', 'FR', 'JP', 'AU', 'IN', 'BR', 'MX']
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header
        writer.writerow([
            'timestamp', 'event_type', 'user_id', 'page_url', 
            'session_id', 'user_agent', 'ip_address', 'country', 'value'
        ])
        
        # Generate data
        for _ in range(num_records):
            timestamp = fake.date_time_between(
                start_date='-30d', 
                end_date='now'
            ).isoformat()
            
            event_type = random.choice(event_types)
            user_id = fake.uuid4()
            page_url = random.choice(pages)
            session_id = fake.uuid4()
            user_agent = fake.user_agent()
            ip_address = fake.ipv4()
            country = random.choice(countries)
            value = random.randint(1, 1000)
            
            writer.writerow([
                timestamp, event_type, user_id, page_url,
                session_id, user_agent, ip_address, country, value
            ])
    
    print(f"Generated {num_records} records in {filename}")

if __name__ == '__main__':
    generate_sample_data('/shared-data/sample_data.csv', 50000)