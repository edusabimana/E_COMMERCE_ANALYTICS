import pymongo
from pymongo import MongoClient
import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, current_date, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from datetime import datetime
import os
import sys
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set the Python executable for PySpark
os.environ['PYSPARK_PYTHON'] = r'C:\Users\edusabimana\AppData\Local\Programs\Python\Python39\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\edusabimana\AppData\Local\Programs\Python\Python39\python.exe'

def initialize_spark():
    try:
        spark = SparkSession.builder \
            .appName("CustomerLifetimeValueAnalysis") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.python.worker.connectionTimeout", "60000") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.driver.port", "7078") \
            .config("spark.ui.port", "4050") \
            .master("local[2]") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to initialize Spark: {str(e)}")
        sys.exit(1)

def get_mongo_data():
    try:
        client = MongoClient('mongodb://localhost:27017')
        db = client['db_ecommerce']
        
        # Aggregate purchase data
        purchase_pipeline = [
            {"$group": {
                "_id": "$user_id",
                "total_spend": {"$sum": "$total"},
                "purchase_count": {"$sum": 1}
            }}
        ]
        purchases = list(db.transactions.aggregate(purchase_pipeline))
        
        # Get user registration dates and IDs
        users = list(db.users.find({}, {'user_id': 1, 'registration_date': 1, '_id': 0}))
        user_ids = set(u['user_id'] for u in users)
        
        logger.info("MongoDB data successfully retrieved")
        return users, purchases, user_ids, client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}")
        sys.exit(1)

def get_user_engagement(user_ids, start_date='2025-03-01', end_date='2025-03-31'):
    connection = None
    try:
        connection = happybase.Connection('localhost')
        connection.open()
        table = connection.table('user_sessions')
        engagement = {}
        
        # Validate and convert dates
        try:
            start_ts = datetime.fromisoformat(end_date).timestamp()
            end_ts = datetime.fromisoformat(start_date).timestamp()
        except ValueError as ve:
            logger.error(f"Invalid date format for start_date or end_date: {str(ve)}")
            sys.exit(1)
        
        for user_id in user_ids:
            start_row = f"{user_id}:{int(999999999999 - start_ts)}"
            end_row = f"{user_id}:{int(999999999999 - end_ts)}"
            rows = table.scan(row_start=start_row, row_stop=end_row)
            session_count = 0
            total_duration = 0
            for _, row in rows:
                try:
                    session_count += 1
                    total_duration += int(row.get(b'session_data:duration', b'0'))
                except Exception as parse_error:
                    logger.warning(f"Skipping malformed row for user {user_id}: {parse_error}")
            engagement[user_id] = {'session_count': session_count, 'total_duration': total_duration}
        
        logger.info("HBase engagement data successfully retrieved")
        return engagement
    except Exception as e:
        logger.error(f"Failed to connect to HBase: {str(e)}")
        sys.exit(1)
    finally:
        if connection:
            connection.close()
            logger.info("HBase connection closed")

def compute_clv(spark):
    try:
        # Load data
        users_data, purchases_data, user_ids, mongo_client = get_mongo_data()
        engagement_data = get_user_engagement(user_ids, '2025-03-01', '2025-03-31')
        
        # Define schemas
        users_schema = StructType([
            StructField('user_id', StringType(), True),
            StructField('registration_date', StringType(), True)
        ])
        purchases_schema = StructType([
            StructField('user_id', StringType(), True),
            StructField('total_spend', DoubleType(), True),
            StructField('purchase_count', IntegerType(), True)
        ])
        engagement_schema = StructType([
            StructField('user_id', StringType(), True),
            StructField('session_count', IntegerType(), True),
            StructField('total_duration', IntegerType(), True)
        ])
        
        # Create DataFrames
        users_df = spark.createDataFrame(users_data, schema=users_schema)
        purchases_df = spark.createDataFrame(purchases_data).select(
            col('_id').alias('user_id'), 'total_spend', 'purchase_count'
        )
        engagement_spark_data = [(k, v['session_count'], v['total_duration']) for k, v in engagement_data.items()]
        engagement_df = spark.createDataFrame(engagement_spark_data, schema=engagement_schema)
        
        # Compute CLV
        clv_df = users_df.join(purchases_df, 'user_id', 'left') \
            .join(engagement_df, 'user_id', 'left') \
            .fillna({'total_spend': 0, 'purchase_count': 0, 'session_count': 0, 'total_duration': 0}) \
            .withColumn('registration_date', col('registration_date').cast(TimestampType())) \
            .withColumn('tenure_days', datediff(current_date(), col('registration_date'))) \
            .withColumn('sessions_per_month', col('session_count') / (col('tenure_days') / 30.0)) \
            .withColumn('avg_purchase_value', 
                when(col('purchase_count') > 0, col('total_spend') / col('purchase_count')).otherwise(0)
            ) \
            .withColumn('engagement_score', 
                col('sessions_per_month') / 10.0 + 
                col('total_duration') / 3600.0
            ) \
            .withColumn('clv', 
                col('avg_purchase_value') * col('session_count') * col('engagement_score')
            ) \
            .na.fill({'clv': 0})
        
        clv_results = clv_df.select(
            'user_id', 'total_spend', 'purchase_count', 'session_count', 
            'total_duration', 'tenure_days', 'engagement_score', 'clv'
        ).orderBy(col('clv').desc())
        
        logger.info("Computed CLV with full details")
        return clv_results, mongo_client
    except Exception as e:
        logger.error(f"Error in compute_clv: {str(e)}")
        raise

def main():
    spark = None
    mongo_client = None
    try:
        spark = initialize_spark()
        clv_results, mongo_client = compute_clv(spark)
        
        # Show results
        logger.info("Top 10 Customers by Estimated CLV:")
        clv_results.show(10, truncate=False)
        
        # Save results
        output_path = r'D:\Personal\masters\BIG DATA ANALYTICS\E_COMMERCE_ANALYTICS\visualizations\clv_results.csv'
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        clv_results.coalesce(1).write.mode('overwrite').option('header', 'true').csv(output_path)
        logger.info(f"CLV saved to: {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to execute main process: {str(e)}")
        sys.exit(1)
    finally:
        if mongo_client:
            mongo_client.close()
            logger.info("MongoDB connection closed")
        if spark:
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()