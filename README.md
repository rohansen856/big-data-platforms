# Big Data Platforms Demo Stack

This repository contains isolated demos for various big data platforms and technologies. Each service is containerized and can be run independently or as part of the complete stack.

## 🏗️ Architecture

```
big-data-platforms/
├── spark/              # Apache Spark - batch and stream processing
├── kafka/              # Apache Kafka - event streaming
├── airflow/            # Apache Airflow - workflow orchestration
├── clickhouse/         # ClickHouse - columnar analytics
├── beam/               # Apache Beam - unified batch and stream processing
├── pig/                # Apache Pig - data analysis platform
├── flink/              # Apache Flink - stream processing engine
├── storm/              # Apache Storm - real-time computation
├── hadoop/             # Apache Hadoop - distributed storage and processing
├── hive/               # Apache Hive - SQL on Hadoop
├── data-generator/     # Unified data generator
├── shared-data/        # Shared data directory
├── docker-compose.yml  # Main orchestration file
├── Makefile           # Professional automation commands
└── requirements.txt    # Python dependencies
```

## 🚀 Quick Start

### Core Services (Recommended)
```bash
# Start essential services (Spark, Kafka, Airflow, ClickHouse)
make up

# Check service status
make status

# View logs
make logs
```

### Individual Services
```bash
# Start specific services using Makefile
make spark-up      # Apache Spark
make kafka-up      # Apache Kafka
make airflow-up    # Apache Airflow
make clickhouse-up # ClickHouse

# Extended services (run individually to save resources)
make beam-up       # Apache Beam
make pig-up        # Apache Pig
make flink-up      # Apache Flink
make storm-up      # Apache Storm
make hadoop-up     # Apache Hadoop
make hive-up       # Apache Hive
```

### Resource Management
The main `docker-compose.yml` includes only the core services by default. Extended services are commented out to reduce resource usage. To use them:

1. **Individual docker-compose files**: Each service has its own `docker-compose.yml` in its directory
2. **Makefile commands**: Use `make service-up` to start individual services
3. **Uncomment in main file**: Edit `docker-compose.yml` to uncomment desired services

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
| Beam Batch | - | Unified batch processing | ✅ Complete |
| Beam Streaming | - | Unified stream processing | ✅ Complete |
| Pig | - | Data analysis platform | ✅ Complete |
| Flink JobManager | 8085 | Stream processing engine | ✅ Complete |
| Storm UI | 8087 | Real-time computation | ✅ Complete |
| Hadoop NameNode | 9870 | Distributed file system | ✅ Complete |
| Hadoop ResourceManager | 8088 | Resource management | ✅ Complete |
| Hive Server2 | 10000/10002 | SQL on Hadoop | ✅ Complete |
| Hive Metastore | 9083 | Metadata management | ✅ Complete |

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
- **Kafka** → **Spark** → **Hive**
- Schema-on-read, data exploration, ad-hoc queries

### 5. Unified Processing with Beam
- **Beam** → **Spark/Flink** → **Multiple sinks**
- Write once, run anywhere batch/stream processing

### 6. Traditional Big Data Analysis
- **Pig** → **Hadoop** → **Analytics**
- High-level data analysis with Pig Latin

### 7. Low-latency Stream Processing
- **Kafka** → **Flink/Storm** → **Real-time dashboards**
- Sub-second processing for critical applications

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
make demo-spark-batch   # Batch processing demo
make demo-spark-stream  # Streaming demo
make demo-spark-ml      # ML pipeline demo
```

### Kafka Streaming
```bash
make demo-kafka-producer  # Start producer
make demo-kafka-consumer  # Start consumer
make demo-kafka-streams   # Stream processor
```

### ClickHouse Analytics
```bash
make demo-clickhouse     # Run analytics demo
make clickhouse-client   # Access SQL client
```

### Apache Beam Processing
```bash
make demo-beam-batch     # Beam batch processing
make demo-beam-stream    # Beam streaming
```

### Apache Pig Analysis
```bash
make demo-pig           # Run Pig Latin analysis
```

### Apache Flink Stream Processing
```bash
make demo-flink         # Run Flink streaming job
```

### Apache Hive SQL Analytics
```bash
make demo-hive          # Run Hive SQL queries
```

### Airflow Workflows
```bash
# Access web UI: http://localhost:8082 (admin/admin)
make airflow-trigger-data  # Trigger data pipeline DAG
make airflow-trigger-ml    # Trigger ML pipeline DAG
```

## 🎯 Learning Objectives

After completing this demo, you will understand:

1. **Distributed Data Processing** with Apache Spark
2. **Event Streaming** with Apache Kafka
3. **Workflow Orchestration** with Apache Airflow
4. **Columnar Analytics** with ClickHouse
5. **Unified Processing** with Apache Beam
6. **Traditional Big Data Analysis** with Apache Pig
7. **Stream Processing** with Apache Flink & Storm
8. **Distributed Storage** with Hadoop HDFS
9. **Data Warehousing** with Apache Hive
10. **Real-time vs Batch Processing** patterns
11. **Data Pipeline Design** and best practices
12. **Containerized Big Data** deployments

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