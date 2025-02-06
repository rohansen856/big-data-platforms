from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

def create_spark_session():
    """Create Spark session for streaming"""
    return SparkSession.builder \
        .appName("StreamingDemo") \
        .config("spark.sql.streaming.checkpointLocation", "/data/checkpoints") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

def process_streaming_data(spark):
    """Process streaming data with Structured Streaming"""
    print("=== Starting Streaming Processing ===")
    
    # Define schema for streaming data
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("session_id", StringType(), True),
        StructField("value", DoubleType(), True)
    ])
    
    # Read streaming data
    streaming_df = spark.readStream \
        .format("json") \
        .schema(schema) \
        .option("path", "/data/streaming_data.json") \
        .load()
    
    # Convert timestamp to proper format
    streaming_df = streaming_df.withColumn("timestamp", 
                                          to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
    
    # Event count by type (tumbling window)
    event_counts = streaming_df \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            window("timestamp", "30 seconds"),
            "event_type"
        ) \
        .agg(count("*").alias("event_count")) \
        .select("window.start", "window.end", "event_type", "event_count")
    
    # Start streaming query for event counts
    query1 = event_counts.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    # Real-time aggregations
    real_time_stats = streaming_df \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(window("timestamp", "1 minute")) \
        .agg(
            count("*").alias("total_events"),
            countDistinct("user_id").alias("unique_users"),
            avg("value").alias("avg_value"),
            max("value").alias("max_value")
        ) \
        .select("window.start", "window.end", "total_events", "unique_users", "avg_value", "max_value")
    
    # Start streaming query for real-time stats
    query2 = real_time_stats.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    # Session analysis
    session_analysis = streaming_df \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            window("timestamp", "2 minutes"),
            "session_id"
        ) \
        .agg(
            count("*").alias("events_in_session"),
            countDistinct("event_type").alias("unique_event_types"),
            sum("value").alias("total_value")
        ) \
        .select("window.start", "window.end", "session_id", "events_in_session", 
                "unique_event_types", "total_value")
    
    # Start streaming query for session analysis
    query3 = session_analysis.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="60 seconds") \
        .start()
    
    return [query1, query2, query3]

def batch_streaming_join(spark):
    """Demonstrate batch-streaming join"""
    print("\n=== Batch-Streaming Join Demo ===")
    
    # Create a static DataFrame (batch)
    product_data = [
        (1, "Laptop", "Electronics", 999.99),
        (2, "Mouse", "Accessories", 29.99),
        (3, "Keyboard", "Accessories", 79.99),
        (4, "Monitor", "Electronics", 299.99),
        (5, "Headphones", "Accessories", 199.99)
    ]
    
    product_schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True)
    ])
    
    products_df = spark.createDataFrame(product_data, product_schema)
    
    # Define streaming schema
    streaming_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("session_id", StringType(), True),
        StructField("value", DoubleType(), True)
    ])
    
    # Read streaming data
    streaming_df = spark.readStream \
        .format("json") \
        .schema(streaming_schema) \
        .option("path", "/data/streaming_data.json") \
        .load()
    
    # Convert timestamp
    streaming_df = streaming_df.withColumn("timestamp", 
                                          to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
    
    # Join streaming with batch data
    enriched_stream = streaming_df.join(products_df, "product_id", "inner") \
        .select("timestamp", "user_id", "event_type", "product_name", 
                "category", "price", "value")
    
    # Aggregate enriched data
    enriched_aggregates = enriched_stream \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            window("timestamp", "1 minute"),
            "category",
            "event_type"
        ) \
        .agg(
            count("*").alias("event_count"),
            sum("value").alias("total_value"),
            avg("price").alias("avg_price")
        ) \
        .select("window.start", "window.end", "category", "event_type", 
                "event_count", "total_value", "avg_price")
    
    # Start enriched streaming query
    query = enriched_aggregates.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    return query

if __name__ == "__main__":
    spark = create_spark_session()
    
    try:
        # Start basic streaming queries
        basic_queries = process_streaming_data(spark)
        
        # Start batch-streaming join
        join_query = batch_streaming_join(spark)
        
        # Wait for all queries
        all_queries = basic_queries + [join_query]
        
        print("\n=== Streaming queries started. Press Ctrl+C to stop ===")
        
        for query in all_queries:
            query.awaitTermination()
            
    except KeyboardInterrupt:
        print("\n=== Stopping streaming queries ===")
        for query in all_queries:
            query.stop()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        spark.stop()