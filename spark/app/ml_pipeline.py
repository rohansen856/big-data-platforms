from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import *
import sys

def create_spark_session():
    """Create Spark session for ML"""
    return SparkSession.builder \
        .appName("MLPipelineDemo") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

def prepare_taxi_ml_data(spark):
    """Prepare taxi data for ML pipeline"""
    print("=== Preparing Taxi Data for ML ===")
    
    # Read taxi data
    taxi_df = spark.read.csv("/data/taxi_data.csv", header=True, inferSchema=True)
    
    # Feature engineering
    taxi_ml_df = taxi_df.withColumn("trip_duration", 
                                   (unix_timestamp("tpep_dropoff_datetime") - 
                                    unix_timestamp("tpep_pickup_datetime")) / 60) \
        .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
        .withColumn("pickup_day_of_week", dayofweek("tpep_pickup_datetime")) \
        .withColumn("speed_mph", 
                   when(col("trip_duration") > 0, 
                        col("trip_distance") / (col("trip_duration") / 60)).otherwise(0)) \
        .withColumn("high_tip", when(col("tip_amount") > 2.0, 1).otherwise(0)) \
        .filter(col("trip_duration") > 0) \
        .filter(col("trip_distance") > 0) \
        .filter(col("fare_amount") > 0)
    
    return taxi_ml_df

def tip_prediction_pipeline(spark):
    """Binary classification: Predict high tips"""
    print("\n=== Tip Prediction Pipeline ===")
    
    taxi_ml_df = prepare_taxi_ml_data(spark)
    
    # Select features for tip prediction
    feature_cols = ["trip_distance", "pickup_hour", "pickup_day_of_week", 
                    "passenger_count", "fare_amount", "trip_duration", "speed_mph"]
    
    # Create feature vector
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    
    # Scale features
    scaler = StandardScaler(inputCol="raw_features", outputCol="features")
    
    # Logistic regression model
    lr = LogisticRegression(featuresCol="features", labelCol="high_tip", 
                           maxIter=10, regParam=0.01)
    
    # Create pipeline
    pipeline = Pipeline(stages=[assembler, scaler, lr])
    
    # Split data
    train_df, test_df = taxi_ml_df.randomSplit([0.8, 0.2], seed=42)
    
    print(f"Training set size: {train_df.count()}")
    print(f"Test set size: {test_df.count()}")
    
    # Train model
    model = pipeline.fit(train_df)
    
    # Make predictions
    predictions = model.transform(test_df)
    
    # Evaluate model
    evaluator = BinaryClassificationEvaluator(labelCol="high_tip", 
                                            rawPredictionCol="rawPrediction")
    auc = evaluator.evaluate(predictions)
    
    print(f"AUC: {auc:.4f}")
    
    # Show predictions
    predictions.select("trip_distance", "fare_amount", "tip_amount", 
                      "high_tip", "prediction", "probability").show(20)
    
    # Feature importance (for Random Forest)
    rf = RandomForestClassifier(featuresCol="features", labelCol="high_tip", 
                               numTrees=10, seed=42)
    rf_pipeline = Pipeline(stages=[assembler, scaler, rf])
    rf_model = rf_pipeline.fit(train_df)
    
    feature_importance = rf_model.stages[-1].featureImportances
    print("\n=== Feature Importance ===")
    for i, importance in enumerate(feature_importance):
        print(f"{feature_cols[i]}: {importance:.4f}")

def fare_prediction_pipeline(spark):
    """Regression: Predict fare amount"""
    print("\n=== Fare Prediction Pipeline ===")
    
    taxi_ml_df = prepare_taxi_ml_data(spark)
    
    # Select features for fare prediction
    feature_cols = ["trip_distance", "pickup_hour", "pickup_day_of_week", 
                    "passenger_count", "trip_duration"]
    
    # Create feature vector
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    
    # Scale features
    scaler = StandardScaler(inputCol="raw_features", outputCol="features")
    
    # Linear regression model
    lr = LinearRegression(featuresCol="features", labelCol="fare_amount", 
                         maxIter=10, regParam=0.01)
    
    # Create pipeline
    pipeline = Pipeline(stages=[assembler, scaler, lr])
    
    # Split data
    train_df, test_df = taxi_ml_df.randomSplit([0.8, 0.2], seed=42)
    
    # Train model
    model = pipeline.fit(train_df)
    
    # Make predictions
    predictions = model.transform(test_df)
    
    # Evaluate model
    evaluator = RegressionEvaluator(labelCol="fare_amount", predictionCol="prediction")
    
    rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
    mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
    r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
    
    print(f"RMSE: {rmse:.4f}")
    print(f"MAE: {mae:.4f}")
    print(f"R²: {r2:.4f}")
    
    # Show predictions
    predictions.select("trip_distance", "trip_duration", "fare_amount", 
                      "prediction").show(20)

def customer_segmentation(spark):
    """Customer segmentation using sales data"""
    print("\n=== Customer Segmentation ===")
    
    # Read sales data
    sales_df = spark.read.csv("/data/sales_data.csv", header=True, inferSchema=True)
    
    # Aggregate customer metrics
    customer_metrics = sales_df.groupBy("customer_id") \
        .agg(
            count("*").alias("total_orders"),
            sum("total_amount").alias("total_spend"),
            avg("total_amount").alias("avg_order_value"),
            max("order_date").alias("last_order_date"),
            countDistinct("product_name").alias("unique_products")
        ) \
        .withColumn("days_since_last_order", 
                   datediff(current_date(), col("last_order_date")))
    
    # Create customer segments
    customer_segments = customer_metrics.withColumn("segment",
        when((col("total_spend") > 1000) & (col("total_orders") > 5), "High Value")
        .when((col("total_spend") > 500) & (col("total_orders") > 3), "Medium Value")
        .when(col("days_since_last_order") > 90, "At Risk")
        .otherwise("Low Value")
    )
    
    # Show segment distribution
    print("\n=== Segment Distribution ===")
    customer_segments.groupBy("segment") \
        .agg(
            count("*").alias("customer_count"),
            avg("total_spend").alias("avg_spend"),
            avg("total_orders").alias("avg_orders")
        ) \
        .show()
    
    # Prepare data for clustering
    feature_cols = ["total_orders", "total_spend", "avg_order_value", 
                    "days_since_last_order", "unique_products"]
    
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features")
    
    # Apply preprocessing
    preprocessed_df = scaler.fit(assembler.transform(customer_metrics)) \
        .transform(assembler.transform(customer_metrics))
    
    print(f"Preprocessed {preprocessed_df.count()} customers for clustering")
    
    # Show sample of preprocessed data
    preprocessed_df.select("customer_id", "total_orders", "total_spend", 
                          "avg_order_value").show(10)

if __name__ == "__main__":
    spark = create_spark_session()
    
    try:
        tip_prediction_pipeline(spark)
        fare_prediction_pipeline(spark)
        customer_segmentation(spark)
        
        print("\n=== ML Pipeline Demo Complete ===")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        spark.stop()