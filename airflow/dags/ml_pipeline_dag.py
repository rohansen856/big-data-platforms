from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
import joblib
import os
import json
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'ml-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'ml_pipeline_dag',
    default_args=default_args,
    description='Machine Learning Pipeline with model training and evaluation',
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    tags=['ml', 'model-training', 'pipeline']
)

def prepare_ml_data(**context):
    """Prepare data for machine learning"""
    logger.info("Starting ML data preparation")
    
    # Generate synthetic dataset for customer churn prediction
    np.random.seed(42)
    n_samples = 5000
    
    # Customer features
    data = {
        'customer_id': [f'CUST-{i:05d}' for i in range(n_samples)],
        'age': np.random.randint(18, 80, n_samples),
        'tenure_months': np.random.randint(1, 72, n_samples),
        'monthly_charges': np.random.uniform(20, 120, n_samples),
        'total_charges': np.random.uniform(100, 8000, n_samples),
        'contract_type': np.random.choice(['Month-to-month', 'One year', 'Two year'], n_samples),
        'payment_method': np.random.choice(['Electronic check', 'Mailed check', 'Bank transfer', 'Credit card'], n_samples),
        'internet_service': np.random.choice(['DSL', 'Fiber optic', 'No'], n_samples),
        'online_security': np.random.choice(['Yes', 'No'], n_samples),
        'tech_support': np.random.choice(['Yes', 'No'], n_samples),
        'streaming_tv': np.random.choice(['Yes', 'No'], n_samples),
        'paperless_billing': np.random.choice(['Yes', 'No'], n_samples),
        'num_services': np.random.randint(1, 10, n_samples),
        'avg_monthly_usage': np.random.uniform(0, 1000, n_samples),
        'customer_service_calls': np.random.randint(0, 20, n_samples),
        'late_payments': np.random.randint(0, 5, n_samples)
    }
    
    df = pd.DataFrame(data)
    
    # Create target variable (churn) based on features
    # Higher churn probability for certain conditions
    churn_probability = (
        0.1 +  # base probability
        0.3 * (df['contract_type'] == 'Month-to-month') +
        0.2 * (df['monthly_charges'] > 80) +
        0.1 * (df['tenure_months'] < 12) +
        0.2 * (df['customer_service_calls'] > 5) +
        0.1 * (df['late_payments'] > 2) +
        0.1 * (df['internet_service'] == 'Fiber optic') +
        np.random.normal(0, 0.1, n_samples)  # Add noise
    )
    
    # Cap probability between 0 and 1
    churn_probability = np.clip(churn_probability, 0, 1)
    df['churn'] = np.random.binomial(1, churn_probability)
    
    # Create additional target for regression (monthly revenue prediction)
    df['predicted_monthly_revenue'] = (
        df['monthly_charges'] * 
        (1 + 0.1 * df['num_services'] + 0.05 * df['tenure_months'] / 12) +
        np.random.normal(0, 10, n_samples)
    )
    
    # Save prepared data
    os.makedirs('/opt/airflow/data/ml', exist_ok=True)
    df.to_csv('/opt/airflow/data/ml/ml_dataset.csv', index=False)
    
    logger.info(f"ML data preparation completed - {len(df)} samples created")
    
    # Store data info
    context['task_instance'].xcom_push(key='data_info', value={
        'total_samples': len(df),
        'features': len(df.columns) - 3,  # excluding customer_id, churn, predicted_monthly_revenue
        'churn_rate': df['churn'].mean(),
        'avg_monthly_revenue': df['predicted_monthly_revenue'].mean()
    })

def feature_engineering(**context):
    """Perform feature engineering"""
    logger.info("Starting feature engineering")
    
    # Load data
    df = pd.read_csv('/opt/airflow/data/ml/ml_dataset.csv')
    
    # Create new features
    df['monthly_charges_per_service'] = df['monthly_charges'] / df['num_services']
    df['total_charges_per_month'] = df['total_charges'] / df['tenure_months']
    df['service_calls_per_month'] = df['customer_service_calls'] / df['tenure_months']
    df['late_payments_per_month'] = df['late_payments'] / df['tenure_months']
    
    # Create categorical features
    df['age_group'] = pd.cut(df['age'], bins=[0, 30, 50, 70, 100], labels=['Young', 'Middle', 'Senior', 'Elderly'])
    df['tenure_group'] = pd.cut(df['tenure_months'], bins=[0, 12, 24, 48, 100], labels=['New', 'Short', 'Medium', 'Long'])
    df['charges_group'] = pd.cut(df['monthly_charges'], bins=[0, 40, 70, 100, 200], labels=['Low', 'Medium', 'High', 'Premium'])
    
    # Create interaction features
    df['contract_payment_interaction'] = df['contract_type'] + '_' + df['payment_method']
    df['internet_security_interaction'] = df['internet_service'] + '_' + df['online_security']
    
    # Handle missing values (if any)
    df.fillna(df.mean() if df.dtypes.name in ['int64', 'float64'] else df.mode().iloc[0], inplace=True)
    
    # Save engineered features
    df.to_csv('/opt/airflow/data/ml/ml_features.csv', index=False)
    
    logger.info("Feature engineering completed")
    
    # Store feature info
    context['task_instance'].xcom_push(key='feature_info', value={
        'total_features': len(df.columns) - 3,  # excluding customer_id, churn, predicted_monthly_revenue
        'new_features_created': 8,
        'categorical_features': 5,
        'numerical_features': len(df.select_dtypes(include=[np.number]).columns) - 3
    })

def train_churn_model(**context):
    """Train customer churn prediction model"""
    logger.info("Starting churn model training")
    
    # Load data
    df = pd.read_csv('/opt/airflow/data/ml/ml_features.csv')
    
    # Prepare features for churn prediction
    feature_columns = [col for col in df.columns if col not in ['customer_id', 'churn', 'predicted_monthly_revenue']]
    
    # Handle categorical variables
    categorical_columns = df[feature_columns].select_dtypes(include=['object']).columns
    df_encoded = df.copy()
    
    label_encoders = {}
    for col in categorical_columns:
        le = LabelEncoder()
        df_encoded[col] = le.fit_transform(df_encoded[col].astype(str))
        label_encoders[col] = le
    
    # Split data
    X = df_encoded[feature_columns]
    y = df_encoded['churn']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Train multiple models
    models = {
        'logistic_regression': LogisticRegression(random_state=42, max_iter=1000),
        'random_forest': RandomForestClassifier(n_estimators=100, random_state=42)
    }
    
    model_results = {}
    
    for model_name, model in models.items():
        logger.info(f"Training {model_name}")
        
        if model_name == 'logistic_regression':
            model.fit(X_train_scaled, y_train)
            y_pred = model.predict(X_test_scaled)
        else:
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)
        
        # Evaluate model
        report = classification_report(y_test, y_pred, output_dict=True)
        
        model_results[model_name] = {
            'accuracy': report['accuracy'],
            'precision': report['1']['precision'],
            'recall': report['1']['recall'],
            'f1_score': report['1']['f1-score']
        }
        
        logger.info(f"{model_name} - Accuracy: {report['accuracy']:.4f}, F1: {report['1']['f1-score']:.4f}")
    
    # Select best model
    best_model_name = max(model_results.keys(), key=lambda x: model_results[x]['f1_score'])
    best_model = models[best_model_name]
    
    # Save model and preprocessing objects
    os.makedirs('/opt/airflow/data/models', exist_ok=True)
    joblib.dump(best_model, '/opt/airflow/data/models/churn_model.pkl')
    joblib.dump(scaler, '/opt/airflow/data/models/churn_scaler.pkl')
    joblib.dump(label_encoders, '/opt/airflow/data/models/churn_label_encoders.pkl')
    
    logger.info(f"Best model ({best_model_name}) saved")
    
    # Store model info
    context['task_instance'].xcom_push(key='churn_model_info', value={
        'best_model': best_model_name,
        'model_results': model_results,
        'training_samples': len(X_train),
        'test_samples': len(X_test)
    })

def train_revenue_model(**context):
    """Train monthly revenue prediction model"""
    logger.info("Starting revenue model training")
    
    # Load data
    df = pd.read_csv('/opt/airflow/data/ml/ml_features.csv')
    
    # Prepare features for revenue prediction
    feature_columns = [col for col in df.columns if col not in ['customer_id', 'churn', 'predicted_monthly_revenue']]
    
    # Handle categorical variables
    categorical_columns = df[feature_columns].select_dtypes(include=['object']).columns
    df_encoded = df.copy()
    
    label_encoders = {}
    for col in categorical_columns:
        le = LabelEncoder()
        df_encoded[col] = le.fit_transform(df_encoded[col].astype(str))
        label_encoders[col] = le
    
    # Split data
    X = df_encoded[feature_columns]
    y = df_encoded['predicted_monthly_revenue']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Train regression model
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    # Make predictions
    y_pred = model.predict(X_test)
    
    # Evaluate model
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    
    logger.info(f"Revenue model - MSE: {mse:.4f}, R2: {r2:.4f}")
    
    # Save model and preprocessing objects
    joblib.dump(model, '/opt/airflow/data/models/revenue_model.pkl')
    joblib.dump(scaler, '/opt/airflow/data/models/revenue_scaler.pkl')
    joblib.dump(label_encoders, '/opt/airflow/data/models/revenue_label_encoders.pkl')
    
    logger.info("Revenue model saved")
    
    # Store model info
    context['task_instance'].xcom_push(key='revenue_model_info', value={
        'mse': mse,
        'r2_score': r2,
        'training_samples': len(X_train),
        'test_samples': len(X_test)
    })

def evaluate_models(**context):
    """Evaluate and compare trained models"""
    logger.info("Starting model evaluation")
    
    # Get model info from previous tasks
    churn_model_info = context['task_instance'].xcom_pull(key='churn_model_info', task_ids='train_churn_model')
    revenue_model_info = context['task_instance'].xcom_pull(key='revenue_model_info', task_ids='train_revenue_model')
    
    # Load test data for additional evaluation
    df = pd.read_csv('/opt/airflow/data/ml/ml_features.csv')
    
    # Feature importance analysis (for Random Forest models)
    feature_columns = [col for col in df.columns if col not in ['customer_id', 'churn', 'predicted_monthly_revenue']]
    
    # Load and analyze churn model
    churn_model = joblib.load('/opt/airflow/data/models/churn_model.pkl')
    
    if hasattr(churn_model, 'feature_importances_'):
        feature_importance = dict(zip(feature_columns, churn_model.feature_importances_))
        top_features = dict(sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)[:10])
    else:
        top_features = {}
    
    # Load and analyze revenue model
    revenue_model = joblib.load('/opt/airflow/data/models/revenue_model.pkl')
    revenue_feature_importance = dict(zip(feature_columns, revenue_model.feature_importances_))
    revenue_top_features = dict(sorted(revenue_feature_importance.items(), key=lambda x: x[1], reverse=True)[:10])
    
    # Create evaluation report
    evaluation_report = {
        'evaluation_date': datetime.now().isoformat(),
        'churn_model': {
            'model_type': churn_model_info['best_model'],
            'performance': churn_model_info['model_results'][churn_model_info['best_model']],
            'top_features': top_features,
            'training_samples': churn_model_info['training_samples']
        },
        'revenue_model': {
            'model_type': 'RandomForestRegressor',
            'mse': revenue_model_info['mse'],
            'r2_score': revenue_model_info['r2_score'],
            'top_features': revenue_top_features,
            'training_samples': revenue_model_info['training_samples']
        }
    }
    
    # Save evaluation report
    os.makedirs('/opt/airflow/data/reports', exist_ok=True)
    with open('/opt/airflow/data/reports/ml_evaluation_report.json', 'w') as f:
        json.dump(evaluation_report, f, indent=2)
    
    logger.info("Model evaluation completed")
    
    # Store evaluation results
    context['task_instance'].xcom_push(key='evaluation_results', value=evaluation_report)

def deploy_models(**context):
    """Deploy models to production-ready format"""
    logger.info("Starting model deployment")
    
    # Create deployment package
    deployment_info = {
        'deployment_date': datetime.now().isoformat(),
        'models': {
            'churn_model': {
                'model_file': '/opt/airflow/data/models/churn_model.pkl',
                'scaler_file': '/opt/airflow/data/models/churn_scaler.pkl',
                'encoders_file': '/opt/airflow/data/models/churn_label_encoders.pkl',
                'model_type': 'classification',
                'target': 'churn'
            },
            'revenue_model': {
                'model_file': '/opt/airflow/data/models/revenue_model.pkl',
                'scaler_file': '/opt/airflow/data/models/revenue_scaler.pkl',
                'encoders_file': '/opt/airflow/data/models/revenue_label_encoders.pkl',
                'model_type': 'regression',
                'target': 'monthly_revenue'
            }
        }
    }
    
    # Save deployment info
    with open('/opt/airflow/data/models/deployment_info.json', 'w') as f:
        json.dump(deployment_info, f, indent=2)
    
    logger.info("Model deployment completed")
    
    # Store deployment info
    context['task_instance'].xcom_push(key='deployment_info', value=deployment_info)

# Define tasks
start_task = DummyOperator(
    task_id='start_ml_pipeline',
    dag=dag
)

prepare_data_task = PythonOperator(
    task_id='prepare_ml_data',
    python_callable=prepare_ml_data,
    dag=dag
)

feature_engineering_task = PythonOperator(
    task_id='feature_engineering',
    python_callable=feature_engineering,
    dag=dag
)

train_churn_task = PythonOperator(
    task_id='train_churn_model',
    python_callable=train_churn_model,
    dag=dag
)

train_revenue_task = PythonOperator(
    task_id='train_revenue_model',
    python_callable=train_revenue_model,
    dag=dag
)

evaluate_task = PythonOperator(
    task_id='evaluate_models',
    python_callable=evaluate_models,
    dag=dag
)

deploy_task = PythonOperator(
    task_id='deploy_models',
    python_callable=deploy_models,
    dag=dag
)

end_task = DummyOperator(
    task_id='end_ml_pipeline',
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE
)

# Task dependencies
start_task >> prepare_data_task >> feature_engineering_task
feature_engineering_task >> [train_churn_task, train_revenue_task]
[train_churn_task, train_revenue_task] >> evaluate_task >> deploy_task >> end_task