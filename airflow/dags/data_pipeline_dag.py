from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import numpy as np
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'data_pipeline_dag',
    default_args=default_args,
    description='Complete ETL pipeline with data quality checks',
    schedule_interval=timedelta(hours=1),
    max_active_runs=1,
    tags=['etl', 'data-quality', 'pipeline']
)

def extract_data(**context):
    """Extract data from multiple sources"""
    logger.info("Starting data extraction")
    
    # Simulate extracting from different sources
    # Source 1: Sales data
    np.random.seed(42)
    sales_data = {
        'order_id': [f'ORD-{i:05d}' for i in range(1000)],
        'customer_id': [f'CUST-{np.random.randint(1, 100):03d}' for _ in range(1000)],
        'product_id': [f'PROD-{np.random.randint(1, 50):02d}' for _ in range(1000)],
        'quantity': np.random.randint(1, 10, 1000),
        'unit_price': np.random.uniform(10, 500, 1000),
        'order_date': pd.date_range('2024-01-01', periods=1000, freq='H')[:1000]
    }
    
    sales_df = pd.DataFrame(sales_data)
    sales_df['total_amount'] = sales_df['quantity'] * sales_df['unit_price']
    
    # Source 2: Customer data
    customer_data = {
        'customer_id': [f'CUST-{i:03d}' for i in range(1, 101)],
        'name': [f'Customer {i}' for i in range(1, 101)],
        'email': [f'customer{i}@example.com' for i in range(1, 101)],
        'city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'], 100),
        'registration_date': pd.date_range('2023-01-01', periods=100, freq='D')[:100]
    }
    
    customers_df = pd.DataFrame(customer_data)
    
    # Source 3: Product data
    product_data = {
        'product_id': [f'PROD-{i:02d}' for i in range(1, 51)],
        'product_name': [f'Product {i}' for i in range(1, 51)],
        'category': np.random.choice(['Electronics', 'Clothing', 'Books', 'Home', 'Sports'], 50),
        'price': np.random.uniform(10, 500, 50)
    }
    
    products_df = pd.DataFrame(product_data)
    
    # Save extracted data
    os.makedirs('/opt/airflow/data/extracted', exist_ok=True)
    sales_df.to_csv('/opt/airflow/data/extracted/sales.csv', index=False)
    customers_df.to_csv('/opt/airflow/data/extracted/customers.csv', index=False)
    products_df.to_csv('/opt/airflow/data/extracted/products.csv', index=False)
    
    logger.info(f"Extracted {len(sales_df)} sales records, {len(customers_df)} customers, {len(products_df)} products")
    
    # Store metadata for next tasks
    context['task_instance'].xcom_push(key='extraction_stats', value={
        'sales_count': len(sales_df),
        'customers_count': len(customers_df),
        'products_count': len(products_df)
    })

def validate_data(**context):
    """Validate extracted data quality"""
    logger.info("Starting data validation")
    
    validation_results = {}
    
    # Validate sales data
    sales_df = pd.read_csv('/opt/airflow/data/extracted/sales.csv')
    
    # Check for missing values
    missing_values = sales_df.isnull().sum()
    validation_results['sales_missing_values'] = missing_values.to_dict()
    
    # Check for duplicates
    duplicates = sales_df.duplicated().sum()
    validation_results['sales_duplicates'] = duplicates
    
    # Check data types
    validation_results['sales_dtypes'] = sales_df.dtypes.to_dict()
    
    # Business rules validation
    negative_quantities = (sales_df['quantity'] < 0).sum()
    negative_prices = (sales_df['unit_price'] < 0).sum()
    validation_results['negative_quantities'] = negative_quantities
    validation_results['negative_prices'] = negative_prices
    
    # Validate customers data
    customers_df = pd.read_csv('/opt/airflow/data/extracted/customers.csv')
    
    # Email format validation
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    invalid_emails = ~customers_df['email'].str.match(email_pattern)
    validation_results['invalid_emails'] = invalid_emails.sum()
    
    # Validate products data
    products_df = pd.read_csv('/opt/airflow/data/extracted/products.csv')
    
    # Check for unique product IDs
    duplicate_product_ids = products_df['product_id'].duplicated().sum()
    validation_results['duplicate_product_ids'] = duplicate_product_ids
    
    # Store validation results
    context['task_instance'].xcom_push(key='validation_results', value=validation_results)
    
    logger.info(f"Data validation completed: {validation_results}")
    
    # Fail if critical issues found
    if negative_quantities > 0 or negative_prices > 0 or duplicate_product_ids > 0:
        raise ValueError("Critical data quality issues found!")

def transform_data(**context):
    """Transform and clean data"""
    logger.info("Starting data transformation")
    
    # Read extracted data
    sales_df = pd.read_csv('/opt/airflow/data/extracted/sales.csv')
    customers_df = pd.read_csv('/opt/airflow/data/extracted/customers.csv')
    products_df = pd.read_csv('/opt/airflow/data/extracted/products.csv')
    
    # Transform sales data
    sales_df['order_date'] = pd.to_datetime(sales_df['order_date'])
    sales_df['order_month'] = sales_df['order_date'].dt.to_period('M')
    sales_df['order_year'] = sales_df['order_date'].dt.year
    sales_df['order_quarter'] = sales_df['order_date'].dt.quarter
    
    # Create revenue categories
    sales_df['revenue_category'] = pd.cut(
        sales_df['total_amount'],
        bins=[0, 50, 200, 500, float('inf')],
        labels=['Low', 'Medium', 'High', 'Premium']
    )
    
    # Transform customer data
    customers_df['registration_date'] = pd.to_datetime(customers_df['registration_date'])
    customers_df['days_since_registration'] = (pd.Timestamp.now() - customers_df['registration_date']).dt.days
    
    # Create customer segments
    customers_df['customer_segment'] = pd.cut(
        customers_df['days_since_registration'],
        bins=[0, 30, 90, 365, float('inf')],
        labels=['New', 'Recent', 'Established', 'Loyal']
    )
    
    # Transform product data
    products_df['price_category'] = pd.cut(
        products_df['price'],
        bins=[0, 50, 200, 500, float('inf')],
        labels=['Budget', 'Mid-range', 'Premium', 'Luxury']
    )
    
    # Create aggregated tables
    # Monthly sales summary
    monthly_sales = sales_df.groupby(['order_month', 'customer_id']).agg({
        'total_amount': 'sum',
        'quantity': 'sum',
        'order_id': 'count'
    }).reset_index()
    monthly_sales.columns = ['month', 'customer_id', 'total_revenue', 'total_quantity', 'order_count']
    
    # Product performance
    product_performance = sales_df.groupby('product_id').agg({
        'quantity': 'sum',
        'total_amount': 'sum',
        'order_id': 'count'
    }).reset_index()
    product_performance.columns = ['product_id', 'total_quantity_sold', 'total_revenue', 'order_count']
    
    # Customer analytics
    customer_analytics = sales_df.groupby('customer_id').agg({
        'total_amount': ['sum', 'mean', 'count'],
        'order_date': ['min', 'max']
    }).reset_index()
    customer_analytics.columns = ['customer_id', 'total_spent', 'avg_order_value', 'order_count', 'first_order', 'last_order']
    
    # Save transformed data
    os.makedirs('/opt/airflow/data/transformed', exist_ok=True)
    sales_df.to_csv('/opt/airflow/data/transformed/sales_transformed.csv', index=False)
    customers_df.to_csv('/opt/airflow/data/transformed/customers_transformed.csv', index=False)
    products_df.to_csv('/opt/airflow/data/transformed/products_transformed.csv', index=False)
    monthly_sales.to_csv('/opt/airflow/data/transformed/monthly_sales.csv', index=False)
    product_performance.to_csv('/opt/airflow/data/transformed/product_performance.csv', index=False)
    customer_analytics.to_csv('/opt/airflow/data/transformed/customer_analytics.csv', index=False)
    
    logger.info("Data transformation completed")
    
    # Store transformation stats
    context['task_instance'].xcom_push(key='transformation_stats', value={
        'transformed_sales': len(sales_df),
        'monthly_summaries': len(monthly_sales),
        'product_performance_records': len(product_performance),
        'customer_analytics_records': len(customer_analytics)
    })

def load_data(**context):
    """Load transformed data to final destination"""
    logger.info("Starting data loading")
    
    # In a real scenario, this would load to a data warehouse
    # For demo purposes, we'll create a consolidated dataset
    
    # Read transformed data
    sales_df = pd.read_csv('/opt/airflow/data/transformed/sales_transformed.csv')
    customers_df = pd.read_csv('/opt/airflow/data/transformed/customers_transformed.csv')
    products_df = pd.read_csv('/opt/airflow/data/transformed/products_transformed.csv')
    
    # Create a consolidated fact table
    fact_table = sales_df.merge(customers_df, on='customer_id', how='left') \
                         .merge(products_df, on='product_id', how='left')
    
    # Create dimension tables
    dim_customers = customers_df[['customer_id', 'name', 'email', 'city', 'customer_segment']].copy()
    dim_products = products_df[['product_id', 'product_name', 'category', 'price_category']].copy()
    dim_time = sales_df[['order_date', 'order_month', 'order_year', 'order_quarter']].drop_duplicates()
    
    # Save to final destination
    os.makedirs('/opt/airflow/data/warehouse', exist_ok=True)
    fact_table.to_csv('/opt/airflow/data/warehouse/fact_sales.csv', index=False)
    dim_customers.to_csv('/opt/airflow/data/warehouse/dim_customers.csv', index=False)
    dim_products.to_csv('/opt/airflow/data/warehouse/dim_products.csv', index=False)
    dim_time.to_csv('/opt/airflow/data/warehouse/dim_time.csv', index=False)
    
    logger.info(f"Data loading completed - {len(fact_table)} records loaded to warehouse")
    
    # Store loading stats
    context['task_instance'].xcom_push(key='loading_stats', value={
        'fact_table_records': len(fact_table),
        'dim_customers_records': len(dim_customers),
        'dim_products_records': len(dim_products),
        'dim_time_records': len(dim_time)
    })

def generate_report(**context):
    """Generate data pipeline report"""
    logger.info("Generating pipeline report")
    
    # Get stats from previous tasks
    extraction_stats = context['task_instance'].xcom_pull(key='extraction_stats', task_ids='extract_data')
    validation_results = context['task_instance'].xcom_pull(key='validation_results', task_ids='validate_data')
    transformation_stats = context['task_instance'].xcom_pull(key='transformation_stats', task_ids='transform_data')
    loading_stats = context['task_instance'].xcom_pull(key='loading_stats', task_ids='load_data')
    
    # Generate report
    report = {
        'pipeline_run_date': datetime.now().isoformat(),
        'extraction': extraction_stats,
        'validation': validation_results,
        'transformation': transformation_stats,
        'loading': loading_stats,
        'status': 'SUCCESS'
    }
    
    # Save report
    os.makedirs('/opt/airflow/data/reports', exist_ok=True)
    import json
    with open('/opt/airflow/data/reports/pipeline_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info("Pipeline report generated successfully")

# Define tasks
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag
)

# Cleanup task
cleanup_task = BashOperator(
    task_id='cleanup_temp_files',
    bash_command='find /opt/airflow/data -name "*.tmp" -delete || true',
    dag=dag
)

end_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE
)

# Task dependencies
start_task >> extract_task >> validate_task >> transform_task >> load_task >> report_task >> cleanup_task >> end_task