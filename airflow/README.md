# Apache Airflow Demo

This demo showcases Apache Airflow's workflow orchestration capabilities with various DAG examples.

## Features

- **ETL Pipelines**: Extract, Transform, Load workflows
- **Data Quality Checks**: Automated data validation
- **ML Pipeline Orchestration**: Machine learning workflows
- **Multi-system Integration**: Connecting different data sources
- **Monitoring & Alerting**: Task failure notifications
- **Dynamic DAGs**: Programmatically generated workflows

## Quick Start

```bash
# Start Airflow
docker-compose up -d

# Access Airflow UI
open http://localhost:8081

# Default credentials: admin/admin

# Trigger a DAG manually
docker-compose exec airflow-webserver airflow dags trigger data_pipeline_dag
```

## Architecture

- **Webserver**: Web UI and API
- **Scheduler**: Task scheduling engine
- **Worker**: Task execution (Celery)
- **Redis**: Message broker
- **PostgreSQL**: Metadata database

## DAGs Included

1. **data_pipeline_dag**: ETL pipeline with data quality checks
2. **ml_pipeline_dag**: Machine learning workflow
3. **sensor_dag**: External system monitoring
4. **dynamic_dag**: Programmatically generated tasks
5. **failure_handling_dag**: Error handling and retries

## Ports

- 8081: Airflow Webserver
- 5555: Flower (Celery monitoring)
- 5432: PostgreSQL
- 6379: Redis