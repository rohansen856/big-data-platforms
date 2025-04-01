# Dataset Information

This document describes the datasets used in the Big Data Platforms demo stack.

## Synthetic Data Generation

All services in this demo use **synthetic data** generated in real-time. This approach ensures:

- **Consistent data flow** across all services
- **No external dependencies** on real datasets
- **Realistic patterns** that simulate production workloads
- **Scalable generation** for performance testing

## Data Sources by Service

### 1. Apache Spark

**Datasets:**
- **NYC Taxi Trip Data (Synthetic)**: 10,000 trip records with pickup/dropoff times, locations, fares, and tips
- **Sales Data (Synthetic)**: 5,000 e-commerce transactions with products, customers, and pricing
- **Customer Data (Synthetic)**: 100 customer profiles with demographics and registration info

**Features:**
- Realistic temporal patterns
- Geographic distribution
- Price variations
- Customer behavior simulation

**Location:** `/spark/data/`

### 2. Apache Kafka

**Event Streams:**
- **Orders**: E-commerce order events with product details, customer info, and payments
- **User Clicks**: Website interaction events with pages, actions, and session data
- **IoT Sensors**: Temperature, humidity, pressure, and vibration readings
- **Financial Transactions**: Payment processing events with risk scoring

**Frequency:**
- Orders: Every 1-2 seconds
- Clicks: Every 0.1-0.5 seconds
- Sensors: Every 5-10 seconds
- Transactions: Every 1-3 seconds

**Location:** Kafka topics (`orders`, `user-clicks`, `iot-sensors`, `transactions`)

### 3. Apache Airflow

**Pipeline Data:**
- **Sales Pipeline**: Extracted from synthetic sales data
- **Customer Pipeline**: Demographics and behavior data
- **ML Pipeline**: Customer churn prediction dataset with 5,000 samples

**Features:**
- Data quality validation
- ETL transformations
- Feature engineering
- Model training data

**Location:** `/airflow/data/`

### 4. ClickHouse

**Analytics Tables:**
- **Events**: Web analytics events (page views, clicks, purchases)
- **Sensor Data**: IoT device readings with timestamps and values
- **Transactions**: Financial transaction records with risk analysis
- **Application Logs**: System and application log entries
- **E-commerce Orders**: Online order processing data

**Volume:** ~1M+ records per day (configurable)

**Location:** ClickHouse database tables in `analytics` schema

### 5. Apache Cassandra

**Distributed Data:**
- **User Sessions**: Session tracking across multiple devices
- **Product Catalog**: Distributed product information
- **Time Series Data**: IoT sensor readings partitioned by time
- **User Profiles**: Customer data distributed across nodes

**Features:**
- Multi-datacenter replication
- Partition key optimization
- Time-based data expiration

**Location:** Cassandra keyspaces and tables

### 6. Apache Hive

**Data Warehouse:**
- **Sales Facts**: Denormalized sales transactions
- **Customer Dimensions**: Customer master data
- **Product Dimensions**: Product catalog information
- **Time Dimensions**: Calendar and time hierarchy

**Features:**
- HDFS storage
- Partitioned tables
- Columnar storage (ORC format)
- SQL-on-Hadoop queries

**Location:** HDFS and Hive metastore

### 7. Apache NiFi

**Data Flows:**
- **File Processing**: CSV and JSON file ingestion
- **Database Synchronization**: Real-time data movement
- **API Integration**: REST API data collection
- **Data Transformation**: Format conversion and enrichment

**Features:**
- Visual flow design
- Data provenance tracking
- Error handling and retry logic
- Real-time monitoring

**Location:** NiFi repositories and flowfiles

## Data Characteristics

### Volume
- **Small Scale**: ~10K-100K records for development
- **Medium Scale**: ~1M-10M records for testing
- **Large Scale**: 100M+ records for performance testing

### Velocity
- **Batch Processing**: Hourly and daily batch jobs
- **Streaming**: Real-time data ingestion (1K-10K events/sec)
- **Micro-batching**: 5-30 second processing windows

### Variety
- **Structured**: CSV, JSON, SQL tables
- **Semi-structured**: Log files, XML documents
- **Unstructured**: Text content, binary files

### Veracity
- **Data Quality**: Built-in validation and cleansing
- **Anomaly Detection**: Synthetic outliers and errors
- **Missing Data**: Realistic null value patterns

## Data Relationships

```
Users → Sessions → Events → Transactions
  ↓       ↓         ↓          ↓
Products → Orders → Payments → Analytics
  ↓       ↓         ↓          ↓
IoT Devices → Sensor Readings → Alerts
```

## External Data Sources (Optional)

For production-like scenarios, you can integrate:

### Real-World Datasets
- **NYC Taxi Data**: [NYC.gov Open Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Stock Market Data**: [Alpha Vantage API](https://www.alphavantage.co/)
- **Weather Data**: [OpenWeatherMap API](https://openweathermap.org/api)
- **Social Media**: [Twitter API](https://developer.twitter.com/en/docs)

### Configuration
1. Update data generators to use external APIs
2. Configure authentication credentials
3. Adjust data schemas and mappings
4. Set up rate limiting and error handling

## Data Governance

### Privacy
- All synthetic data is anonymized
- No real personal information is used
- GDPR-compliant data handling

### Security
- Data encryption in transit and at rest
- Access control and authentication
- Audit logging for all data operations

### Compliance
- Data retention policies
- Backup and recovery procedures
- Disaster recovery planning

## Performance Optimization

### Indexing
- Primary and secondary indexes
- Composite indexes for complex queries
- Bloom filters for fast lookups

### Partitioning
- Time-based partitioning
- Hash-based distribution
- Range-based segmentation

### Caching
- In-memory caching layers
- Materialized views
- Pre-computed aggregations

## Monitoring and Observability

### Data Quality Metrics
- Record counts and data volumes
- Schema validation results
- Data freshness indicators

### System Metrics
- Throughput and latency
- Error rates and success rates
- Resource utilization

### Alerting
- Data pipeline failures
- Quality threshold breaches
- System performance degradation

## Scaling Considerations

### Horizontal Scaling
- Add more worker nodes
- Increase partition counts
- Distribute across availability zones

### Vertical Scaling
- Increase memory and CPU
- Add more storage capacity
- Optimize query performance

### Cost Optimization
- Data lifecycle management
- Compression and archival
- Resource scheduling and auto-scaling