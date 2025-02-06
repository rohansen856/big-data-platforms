from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

def create_spark_session():
    """Create Spark session with optimized configuration"""
    return SparkSession.builder \
        .appName("BatchProcessingDemo") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def analyze_taxi_data(spark):
    """Analyze NYC taxi trip data"""
    print("=== Taxi Data Analysis ===")
    
    # Read taxi data
    taxi_df = spark.read.csv("/data/taxi_data.csv", header=True, inferSchema=True)
    
    print(f"Total records: {taxi_df.count()}")
    print("\nSchema:")
    taxi_df.printSchema()
    
    # Basic statistics
    print("\n=== Trip Statistics ===")
    taxi_df.select(
        avg("trip_distance").alias("avg_distance"),
        avg("fare_amount").alias("avg_fare"),
        avg("tip_amount").alias("avg_tip"),
        max("trip_distance").alias("max_distance"),
        min("trip_distance").alias("min_distance")
    ).show()
    
    # Hourly trip analysis
    print("\n=== Hourly Trip Analysis ===")
    taxi_df.withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
        .groupBy("pickup_hour") \
        .agg(count("*").alias("trip_count"),
             avg("fare_amount").alias("avg_fare")) \
        .orderBy("pickup_hour") \
        .show(24)
    
    # Payment type analysis
    print("\n=== Payment Type Analysis ===")
    taxi_df.groupBy("payment_type") \
        .agg(count("*").alias("count"),
             avg("fare_amount").alias("avg_fare"),
             avg("tip_amount").alias("avg_tip")) \
        .orderBy("count", ascending=False) \
        .show()
    
    # Distance vs Fare correlation
    print("\n=== Distance vs Fare Analysis ===")
    taxi_df.select(
        when(col("trip_distance") < 2, "Short (< 2 miles)")
        .when(col("trip_distance") < 5, "Medium (2-5 miles)")
        .when(col("trip_distance") < 10, "Long (5-10 miles)")
        .otherwise("Very Long (>10 miles)").alias("distance_category")
    ).groupBy("distance_category") \
        .agg(count("*").alias("trip_count"),
             avg("fare_amount").alias("avg_fare")) \
        .orderBy("avg_fare", ascending=False) \
        .show()

def analyze_sales_data(spark):
    """Analyze sales data"""
    print("\n=== Sales Data Analysis ===")
    
    # Read sales data
    sales_df = spark.read.csv("/data/sales_data.csv", header=True, inferSchema=True)
    
    print(f"Total sales records: {sales_df.count()}")
    
    # Sales by category
    print("\n=== Sales by Category ===")
    sales_df.groupBy("category") \
        .agg(count("*").alias("order_count"),
             sum("total_amount").alias("total_revenue"),
             avg("total_amount").alias("avg_order_value")) \
        .orderBy("total_revenue", ascending=False) \
        .show()
    
    # Top products
    print("\n=== Top Products by Revenue ===")
    sales_df.groupBy("product_name") \
        .agg(count("*").alias("order_count"),
             sum("total_amount").alias("total_revenue")) \
        .orderBy("total_revenue", ascending=False) \
        .show(10)
    
    # Monthly sales trend
    print("\n=== Monthly Sales Trend ===")
    sales_df.withColumn("month", month("order_date")) \
        .groupBy("month") \
        .agg(count("*").alias("order_count"),
             sum("total_amount").alias("total_revenue")) \
        .orderBy("month") \
        .show()
    
    # Customer age analysis
    print("\n=== Customer Age Analysis ===")
    sales_df.select(
        when(col("customer_age") < 25, "18-24")
        .when(col("customer_age") < 35, "25-34")
        .when(col("customer_age") < 45, "35-44")
        .when(col("customer_age") < 55, "45-54")
        .otherwise("55+").alias("age_group")
    ).groupBy("age_group") \
        .agg(count("*").alias("customer_count"),
             avg("total_amount").alias("avg_spend")) \
        .orderBy("avg_spend", ascending=False) \
        .show()

def advanced_analytics(spark):
    """Advanced analytics with window functions"""
    print("\n=== Advanced Analytics ===")
    
    sales_df = spark.read.csv("/data/sales_data.csv", header=True, inferSchema=True)
    
    # Window functions for ranking
    from pyspark.sql.window import Window
    
    window_spec = Window.partitionBy("category").orderBy(col("total_amount").desc())
    
    print("\n=== Top 3 Orders per Category ===")
    sales_df.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 3) \
        .select("category", "product_name", "total_amount", "rank") \
        .orderBy("category", "rank") \
        .show()
    
    # Running totals
    window_spec_running = Window.partitionBy("category").orderBy("order_date")
    
    print("\n=== Running Totals by Category ===")
    sales_df.withColumn("running_total", 
                       sum("total_amount").over(window_spec_running)) \
        .select("category", "order_date", "total_amount", "running_total") \
        .orderBy("category", "order_date") \
        .show(20)

if __name__ == "__main__":
    spark = create_spark_session()
    
    try:
        analyze_taxi_data(spark)
        analyze_sales_data(spark)
        advanced_analytics(spark)
        
        print("\n=== Batch Processing Complete ===")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        spark.stop()