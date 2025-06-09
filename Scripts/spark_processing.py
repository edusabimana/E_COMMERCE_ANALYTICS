from pyspark.sql import SparkSession
import logging
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def initialize_spark():
    """Initialize and return a SparkSession."""
    try:
        spark = SparkSession.builder \
            .appName("ECommerceAnalytics") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.debug.maxToStringFields", "100") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.driver.port", "7078") \
            .config("spark.ui.port", "4050") \
            .config("spark.pyspark.python", "C:/Users/edusabimana/AppData/Local/Programs/Python/Python39/python.exe") \
            .config("spark.executor.heartbeatInterval", "60s") \
            .config("spark.network.timeout", "120s") \
            .master("local[*]") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Error initializing SparkSession: {str(e)}")
        raise

def top_selling_products(spark):
    """Fetch top 10 selling products by revenue."""
    try:
        data_dir = r'D:\Personal\masters\BIG DATA ANALYTICS\E_COMMERCE_ANALYTICS\data'
        transactions_path = os.path.join(data_dir, 'transactions.json')
        
        if not os.path.exists(data_dir):
            logger.error(f"Data directory does not exist: {data_dir}")
            raise FileNotFoundError(f"Data directory does not exist: {data_dir}")
        if not os.path.exists(transactions_path):
            logger.error(f"Transactions JSON file not found: {transactions_path}")
            raise FileNotFoundError(f"Transactions JSON file not found: {transactions_path}")

        logger.info(f"Reading transactions from: {transactions_path}")
        transactions_df = spark.read.json(transactions_path).cache()
        logger.info("Transactions DataFrame schema:")
        transactions_df.printSchema()

        transactions_df.createOrReplaceTempView('transactions')
        
        query = """
        SELECT product_id AS _id, 
               SUM(total) AS total_revenue, 
               SUM(quantity) AS total_quantity
        FROM transactions
        GROUP BY product_id
        ORDER BY total_revenue DESC
        LIMIT 10
        """
        logger.info("Executing top_selling_products query")
        result = spark.sql(query)
        result.cache()
        
        result_count = result.count()
        if result_count == 0:
            logger.warning("No data returned from top_selling_products query")
            return []
        
        logger.info("Top-selling products:")
        for row in result.collect():
            logger.info(row.asDict())
        
        return result.collect()
    except Exception as e:
        logger.error(f"Error in top_selling_products: {str(e)}")
        return []

def revenue_by_category(spark):
    """Fetch revenue by product category."""
    try:
        data_dir = r'D:\Personal\masters\BIG DATA ANALYTICS\E_COMMERCE_ANALYTICS\data'
        transactions_path = os.path.join(data_dir, 'transactions.json')
        
        if not os.path.exists(data_dir):
            logger.error(f"Data directory does not exist: {data_dir}")
            raise FileNotFoundError(f"Data directory does not exist: {data_dir}")
        if not os.path.exists(transactions_path):
            logger.error(f"Transactions JSON file not found: {transactions_path}")
            raise FileNotFoundError(f"Transactions JSON file not found: {transactions_path}")

        logger.info(f"Reading transactions from: {transactions_path}")
        transactions_df = spark.read.json(transactions_path).cache()
        logger.info("Transactions DataFrame schema:")
        transactions_df.printSchema()

        transactions_df.createOrReplaceTempView('transactions')
        
        query = """
        SELECT category_id AS _id, 
               SUM(total) AS total_revenue
        FROM transactions
        GROUP BY category_id
        ORDER BY total_revenue DESC
        """
        logger.info("Executing revenue_by_category query")
        result = spark.sql(query)
        result.cache()
        
        result_count = result.count()
        if result_count == 0:
            logger.warning("No data returned from revenue_by_category query")
            return []
        
        logger.info("Revenue by category:")
        for row in result.collect():
            logger.info(row.asDict())
        
        return result.collect()
    except Exception as e:
        logger.error(f"Error in revenue_by_category: {str(e)}")
        return []

def revenue_by_state(spark):
    """Fetch top 10 states by revenue."""
    try:
        data_dir = r'D:\Personal\masters\BIG DATA ANALYTICS\E_COMMERCE_ANALYTICS\data'
        users_path = os.path.join(data_dir, 'users.json')
        transactions_path = os.path.join(data_dir, 'transactions.json')

        if not os.path.exists(data_dir):
            logger.error(f"Data directory does not exist: {data_dir}")
            raise FileNotFoundError(f"Data directory does not exist: {data_dir}")
        if not os.path.exists(users_path):
            logger.error(f"Users JSON file not found: {users_path}")
            raise FileNotFoundError(f"Users JSON file not found: {users_path}")
        if not os.path.exists(transactions_path):
            logger.error(f"Transactions JSON file not found: {transactions_path}")
            raise FileNotFoundError(f"Transactions JSON file not found: {transactions_path}")

        logger.info(f"Reading users from: {users_path}")
        logger.info(f"Reading transactions from: {transactions_path}")
        
        users_df = spark.read.json(users_path).cache()
        transactions_df = spark.read.json(transactions_path).cache()

        logger.info("Users DataFrame schema:")
        users_df.printSchema()
        logger.info("Transactions DataFrame schema:")
        transactions_df.printSchema()

        users_df.createOrReplaceTempView('users')
        transactions_df.createOrReplaceTempView('transactions')

        query = """
        SELECT u.geo_data.state AS _id, 
               SUM(t.total) AS total_revenue
        FROM users u
        JOIN transactions t ON u.user_id = t.user_id
        GROUP BY u.geo_data.state
        ORDER BY total_revenue DESC
        LIMIT 10
        """
        logger.info("Executing revenue_by_state query")
        result = spark.sql(query)
        result.cache()
        
        result_count = result.count()
        if result_count == 0:
            logger.warning("No data returned from revenue_by_state query")
            return []
        
        logger.info("Top 10 states by revenue:")
        for row in result.collect():
            logger.info(row.asDict())
        
        return result.collect()
    except Exception as e:
        logger.error(f"Error in revenue_by_state: {str(e)}")
        return []

if __name__ == "__main__":
    spark = None
    try:
        spark = initialize_spark()
        
        # Execute analytics
        top_products = top_selling_products(spark)
        category_revenue = revenue_by_category(spark)
        state_revenue = revenue_by_state(spark)
    except Exception as e:
        logger.error(f"Failed to execute main process: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")