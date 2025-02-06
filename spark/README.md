# Apache Spark Demo

This demo showcases Apache Spark's capabilities for both batch and stream processing using PySpark.

## Features

- **Batch Processing**: Sales data analysis with aggregations and transformations
- **Stream Processing**: Real-time data processing with Structured Streaming
- **MLlib Integration**: Machine learning pipeline example
- **Data Sources**: NYC Yellow Taxi Trip Records (real-world dataset)

## Quick Start

```bash
# Start Spark cluster
docker-compose up -d

# Access Spark UI
open http://localhost:8080

# Run batch processing demo
docker-compose exec spark-master python /app/batch_processing.py

# Run streaming demo
docker-compose exec spark-master python /app/streaming_demo.py
```

## Architecture

- **Spark Master**: Cluster manager
- **Spark Worker**: Processing nodes
- **Jupyter**: Interactive development environment
- **Data Generator**: Simulates streaming data

## Data Sources

- **NYC Taxi Data**: Real-world dataset for batch processing
- **Synthetic Sales Data**: Generated for streaming examples
- **Customer Data**: Faker-generated for ML examples

## Ports

- 8080: Spark Master UI
- 8081: Spark Worker UI
- 8888: Jupyter Notebook
- 4040: Spark Application UI