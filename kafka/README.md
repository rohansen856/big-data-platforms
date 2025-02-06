# Apache Kafka Demo

This demo showcases Apache Kafka's event streaming capabilities with producers, consumers, and stream processing.

## Features

- **Event Streaming**: Real-time data ingestion and processing
- **Multiple Topics**: Different data streams (orders, clicks, sensors)
- **Consumer Groups**: Parallel processing and load balancing
- **Kafka Streams**: Stream processing applications
- **Schema Registry**: Data schema management
- **Kafka Connect**: Data integration

## Quick Start

```bash
# Start Kafka cluster
docker-compose up -d

# Create topics
docker-compose exec kafka kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Run producer
docker-compose exec kafka python /app/producer.py

# Run consumer
docker-compose exec kafka python /app/consumer.py
```

## Architecture

- **Zookeeper**: Coordination service
- **Kafka Broker**: Message broker
- **Schema Registry**: Schema management
- **Kafka Connect**: Data integration
- **Kafka UI**: Web interface

## Data Sources

- **E-commerce Orders**: Real-time order processing
- **User Clicks**: Website interaction events
- **IoT Sensors**: Temperature and humidity data
- **Financial Transactions**: Payment processing events

## Ports

- 9092: Kafka Broker
- 2181: Zookeeper
- 8081: Schema Registry
- 8080: Kafka UI
- 8083: Kafka Connect