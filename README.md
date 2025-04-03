# Big Data Platforms Demo Stack

This repository contains isolated demos for various big data platforms and technologies. Each service is containerized and can be run independently or as part of the complete stack.

## 🏗️ Architecture

```
big-data-platforms/
├── spark/              # Apache Spark - batch and stream processing
├── kafka/              # Apache Kafka - event streaming
├── airflow/            # Apache Airflow - workflow orchestration
├── clickhouse/         # ClickHouse - columnar analytics
├── cassandra/          # Apache Cassandra - distributed NoSQL (TODO)
├── hive/               # Apache Hive - SQL on Hadoop (TODO)
├── nifi/               # Apache NiFi - dataflow automation (TODO)
├── data-generator/     # Unified data generator
├── shared-data/        # Shared data directory
├── docker-compose.yml  # Main orchestration file
├── setup.sh           # Setup script
└── requirements.txt    # Python dependencies
```

## 🚀 Quick Start

### Automated Setup
```bash
# Clone and run setup script
./setup.sh
```

### Manual Setup
```bash
# Start complete stack
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f [service-name]
```

### Individual Services
```bash
# Start specific service
cd spark && docker-compose up -d
cd kafka && docker-compose up -d
cd airflow && docker-compose up -d
cd clickhouse && docker-compose up -d
```

## 📊 Services Overview

| Service | Port | Purpose | Status |
|---------|------|---------|--------|
| Spark Master | 8080 | Batch and stream processing | ✅ Complete |
| Spark Worker | 8081 | Spark worker node | ✅ Complete |
| Kafka | 9092 | Event streaming | ✅ Complete |
| Kafka UI | 8090 | Kafka management interface | ✅ Complete |
| Airflow | 8082 | Workflow orchestration | ✅ Complete |
| ClickHouse | 8123/9000 | Columnar analytics | ✅ Complete |
| Tabix | 8091 | ClickHouse web interface | ✅ Complete |
| Jupyter | 8888 | Interactive notebooks | ✅ Complete |
| Cassandra | 9042 | Distributed NoSQL | 🔄 TODO |
| Hive | 10000 | SQL on Hadoop | 🔄 TODO |
| NiFi | 8443 | Dataflow automation | 🔄 TODO |

## 🎯 Use Cases Demonstrated

### 1. Real-time Analytics Pipeline
- **Kafka** → **Spark Streaming** → **ClickHouse**
- Event ingestion, stream processing, real-time dashboards

### 2. Batch Processing Workflow
- **Airflow** → **Spark** → **ClickHouse**
- ETL pipelines, data quality checks, scheduled processing

### 3. Machine Learning Pipeline
- **Airflow** → **Spark MLlib** → Model deployment
- Feature engineering, model training, automated retraining

### 4. Data Lake Analytics
- **Kafka** → **Spark** → **Hive** (TODO)
- Schema-on-read, data exploration, ad-hoc queries

## 📋 Data Sources

All services use **synthetic data** generated in real-time:
- **Web Analytics**: Page views, clicks, user sessions
- **IoT Sensors**: Temperature, humidity, pressure readings
- **Financial Transactions**: Payments, fraud detection
- **E-commerce Orders**: Product sales, customer behavior
- **Application Logs**: System metrics, error tracking

See [DATASET.md](DATASET.md) for detailed information.

## 🔧 Prerequisites

- **Docker**: 20.10+
- **Docker Compose**: 1.29+
- **System Requirements**: 8GB RAM, 4 CPU cores
- **Storage**: 10GB free space

## 📚 Documentation

- [Setup Guide](setup.sh) - Automated setup script
- [Dataset Information](DATASET.md) - Data sources and schemas
- [Spark Demo](spark/README.md) - Batch and stream processing
- [Kafka Demo](kafka/README.md) - Event streaming
- [Airflow Demo](airflow/README.md) - Workflow orchestration
- [ClickHouse Demo](clickhouse/README.md) - Columnar analytics

## 🎮 Interactive Demos

### Spark Processing
```bash
# Batch processing demo
docker-compose exec spark-master python /app/batch_processing.py

# Streaming demo
docker-compose exec spark-master python /app/streaming_demo.py

# ML pipeline demo
docker-compose exec spark-master python /app/ml_pipeline.py
```

### Kafka Streaming
```bash
# Start producer
docker-compose exec kafka python /app/producer.py

# Start consumer
docker-compose exec kafka python /app/consumer.py

# Stream processor
docker-compose exec kafka python /app/streams_processor.py
```

### ClickHouse Analytics
```bash
# Run analytics demo
docker-compose exec clickhouse python /app/analytics_demo.py

# Access SQL client
docker-compose exec clickhouse clickhouse-client
```

### Airflow Workflows
```bash
# Access web UI: http://localhost:8082
# Login: admin/admin

# Trigger DAG manually
docker-compose exec airflow-webserver airflow dags trigger data_pipeline_dag
```

## 🎯 Learning Objectives

After completing this demo, you will understand:

1. **Distributed Data Processing** with Apache Spark
2. **Event Streaming** with Apache Kafka
3. **Workflow Orchestration** with Apache Airflow
4. **Columnar Analytics** with ClickHouse
5. **Real-time vs Batch Processing** patterns
6. **Data Pipeline Design** and best practices
7. **Containerized Big Data** deployments

## 🔍 Monitoring and Observability

### Service Health Checks
```bash
# Check all services
docker-compose ps

# Service-specific health
curl http://localhost:8080  # Spark Master
curl http://localhost:8123/ping  # ClickHouse
```

### Resource Monitoring
```bash
# Container resource usage
docker stats

# Logs monitoring
docker-compose logs -f --tail=100 [service-name]
```

## 🚨 Troubleshooting

### Common Issues

1. **Port Conflicts**: Check if ports 8080-8092 are available
2. **Memory Issues**: Ensure at least 8GB RAM is available
3. **Docker Issues**: Restart Docker daemon if containers fail to start
4. **Permission Issues**: Run setup script with appropriate permissions

### Reset Environment
```bash
# Stop all services
docker-compose down

# Remove volumes and data
docker-compose down -v

# Clean up containers
docker system prune -a
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add your service demo
4. Update documentation
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Apache Software Foundation for the open-source tools
- Docker community for containerization
- Big Data community for best practices and patterns