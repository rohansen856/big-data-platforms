# Apache Beam Demo

This directory contains Apache Beam pipeline examples for both batch and streaming data processing.

## Overview

Apache Beam is a unified programming model for both batch and streaming data processing. It provides:

- **Unified API**: Same code works for batch and streaming
- **Portability**: Run on multiple execution engines (Spark, Flink, Dataflow)
- **Extensibility**: Custom transforms and I/O connectors

## Services

### Beam Batch Processor
- **Container**: `beam-batch-processor`
- **Purpose**: Processes batch data from files
- **Input**: JSON files from shared data
- **Output**: Aggregated results

### Beam Streaming Processor  
- **Container**: `beam-streaming-processor`
- **Purpose**: Processes streaming data from Kafka
- **Input**: Kafka topics
- **Output**: Real-time aggregations

## Pipeline Examples

### Batch Pipeline (`batch_pipeline.py`)
```python
# Read from file -> Parse JSON -> Filter -> Group -> Aggregate -> Write
```

### Streaming Pipeline (`streaming_pipeline.py`)
```python
# Read from Kafka -> Process -> Window -> Aggregate -> Write to Kafka/File
```

## Running the Services

### Start All Services
```bash
docker-compose up -d
```

### Run Batch Pipeline Only
```bash
docker-compose run --rm beam-batch python batch_pipeline.py
```

### Run Streaming Pipeline Only
```bash
docker-compose run --rm beam-streaming python streaming_pipeline.py
```

## Data Flow

1. **Batch Processing**:
   - Input: `/shared-data/sample_data.json`
   - Processing: Parse, filter, group, aggregate
   - Output: `/shared-data/beam_output-*.json`

2. **Streaming Processing**:
   - Input: Kafka topic `web-events`
   - Processing: Real-time windowed aggregation
   - Output: Kafka topic `beam-aggregated-events` + files

## Monitoring

### View Logs
```bash
docker-compose logs -f beam-batch
docker-compose logs -f beam-streaming
```

### Check Output Files
```bash
ls -la ../shared-data/beam_*
```

## Configuration

Environment variables are configured in `.env`:
- `BEAM_RUNNER`: Execution engine (DirectRunner, FlinkRunner, etc.)
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka connection
- `WINDOW_SIZE_SECONDS`: Streaming window size

## Integration

This Beam setup integrates with:
- **Kafka**: For streaming data input/output
- **Spark**: For distributed processing
- **Shared Data**: For batch file processing
- **Main Stack**: Through shared network and volumes