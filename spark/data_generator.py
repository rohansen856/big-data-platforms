import pandas as pd
import numpy as np
from faker import Faker
import json
import time
import os
from datetime import datetime, timedelta
import random

fake = Faker()

def generate_taxi_data(num_records=10000):
    """Generate synthetic NYC taxi trip data"""
    data = []
    
    for _ in range(num_records):
        pickup_datetime = fake.date_time_between(start_date='-1y', end_date='now')
        trip_duration = timedelta(minutes=random.randint(5, 120))
        dropoff_datetime = pickup_datetime + trip_duration
        
        record = {
            'VendorID': random.randint(1, 2),
            'tpep_pickup_datetime': pickup_datetime.isoformat(),
            'tpep_dropoff_datetime': dropoff_datetime.isoformat(),
            'passenger_count': random.randint(1, 6),
            'trip_distance': round(random.uniform(0.5, 20.0), 2),
            'pickup_longitude': round(random.uniform(-74.05, -73.75), 6),
            'pickup_latitude': round(random.uniform(40.63, 40.85), 6),
            'dropoff_longitude': round(random.uniform(-74.05, -73.75), 6),
            'dropoff_latitude': round(random.uniform(40.63, 40.85), 6),
            'payment_type': random.randint(1, 4),
            'fare_amount': round(random.uniform(2.5, 50.0), 2),
            'extra': round(random.uniform(0, 1.0), 2),
            'mta_tax': 0.5,
            'tip_amount': round(random.uniform(0, 10.0), 2),
            'tolls_amount': round(random.uniform(0, 5.0), 2),
            'improvement_surcharge': 0.3,
            'total_amount': 0
        }
        
        record['total_amount'] = (record['fare_amount'] + record['extra'] + 
                                record['mta_tax'] + record['tip_amount'] + 
                                record['tolls_amount'] + record['improvement_surcharge'])
        
        data.append(record)
    
    return pd.DataFrame(data)

def generate_sales_data(num_records=5000):
    """Generate synthetic sales data"""
    data = []
    
    products = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones', 'Tablet', 'Phone', 'Charger']
    categories = ['Electronics', 'Accessories', 'Mobile', 'Computing']
    
    for _ in range(num_records):
        record = {
            'order_id': fake.uuid4(),
            'customer_id': fake.uuid4(),
            'product_name': random.choice(products),
            'category': random.choice(categories),
            'quantity': random.randint(1, 5),
            'unit_price': round(random.uniform(10, 1000), 2),
            'discount': round(random.uniform(0, 0.3), 2),
            'order_date': fake.date_between(start_date='-1y', end_date='today').isoformat(),
            'customer_age': random.randint(18, 70),
            'customer_city': fake.city(),
            'customer_state': fake.state()
        }
        
        record['total_amount'] = record['quantity'] * record['unit_price'] * (1 - record['discount'])
        data.append(record)
    
    return pd.DataFrame(data)

def generate_streaming_data():
    """Generate streaming data continuously"""
    while True:
        record = {
            'timestamp': datetime.now().isoformat(),
            'user_id': fake.uuid4(),
            'event_type': random.choice(['click', 'view', 'purchase', 'login', 'logout']),
            'product_id': random.randint(1, 1000),
            'session_id': fake.uuid4(),
            'value': round(random.uniform(0, 100), 2)
        }
        
        with open('/data/streaming_data.json', 'a') as f:
            f.write(json.dumps(record) + '\n')
        
        time.sleep(1)

if __name__ == "__main__":
    os.makedirs('/data', exist_ok=True)
    
    print("Generating taxi data...")
    taxi_df = generate_taxi_data(10000)
    taxi_df.to_csv('/data/taxi_data.csv', index=False)
    
    print("Generating sales data...")
    sales_df = generate_sales_data(5000)
    sales_df.to_csv('/data/sales_data.csv', index=False)
    
    print("Starting streaming data generation...")
    generate_streaming_data()