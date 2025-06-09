import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
import happybase
from pymongo import MongoClient
import matplotlib.dates as mdates
import time

# HBase write with retry mechanism
def write_to_hbase_with_retry(table, data, max_retries=3, retry_delay=10):
    start_time = time.time()
    for attempt in range(max_retries):
        try:
            batch = table.batch()
            for key, value in data.items():
                batch.put(key, value)
            batch.send()
            duration = time.time() - start_time
            print(f"HBase write completed in {duration:.2f} seconds")
            return True
        except Exception as e:
            print(f"HBase write attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            return False
    return False

# Initialize Spark session with increased resources
spark = SparkSession.builder.appName("Visualizations") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "6g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.pyspark.python", "python3") \
    .config("spark.pyspark.driver.python", "python3") \
    .getOrCreate()

# Load and register transactions, products, and users data
try:
    print("Loading transactions data...")
    transactions_df = spark.read.json("data/transactions.json").limit(10000)
    transactions_df = transactions_df.na.fill({"discount": 0.0, "session_id": "none"})
    transactions_df.createOrReplaceTempView("transactions")
    transactions_df.cache()
    print("Transactions loaded")

    print("Loading products data...")
    products_df = spark.read.json("data/products.json").limit(1000)
    products_df.createOrReplaceTempView("products")
    products_df.cache()
    print("Products loaded")

    print("Loading users data...")
    users_df = spark.read.json("data/users.json").limit(1000)
    users_df.createOrReplaceTempView("users")
    users_df.cache()
    print("Users loaded")
except Exception as e:
    print(f"Data loading failed: {e}")
    spark.stop()
    exit(1)

# Sales by category: Aggregates transaction totals by product category (Spark)
try:
    print("Running sales by category query...")
    df = spark.sql("""
        WITH exploded_transactions AS (
            SELECT t.total, item.product_id AS item_product_id
            FROM transactions t
            LATERAL VIEW explode(t.items) AS item
        )
        SELECT p.category_id, SUM(t.total) as total_revenue
        FROM exploded_transactions t
        JOIN products p ON t.item_product_id = p.product_id
        GROUP BY p.category_id
    """).toPandas()
    print(f"Sales by category query completed. DataFrame size: {len(df)} rows")
    if df.empty:
        print("Warning: Sales by category DataFrame is empty")
    else:
        plt.figure(figsize=(10, 6))
        sns.barplot(x="total_revenue", y="category_id", data=df)
        plt.title("Sales Performance by Category (Spark)")
        try:
            plt.savefig("visualizations/sales_by_category_spark.png")
            print("Saved sales by category (Spark) plot")
        except Exception as e:
            print(f"Failed to save sales by category plot: {e}")
        plt.close()

        print("Writing sales by category to HBase...")
        try:
            connection = happybase.Connection("localhost", port=9090, timeout=60000)
            table = connection.table("sales_by_category")
            data = {row["category_id"]: {"revenue:total": str(row["total_revenue"])} for _, row in df.iterrows()}
            if write_to_hbase_with_retry(table, data):
                print("Wrote sales by category (Spark) to HBase")
            else:
                print("Failed to write sales by category to HBase after retries")
            connection.close()
        except Exception as e:
            print(f"HBase write for sales by category failed: {e}")
except Exception as e:
    print(f"Sales by category query failed: {e}")

# Conversion funnel: Counts sessions by conversion status
try:
    print("Loading sessions data for conversion funnel...")
    sessions_df = spark.read.json("data/sessions_0.json").limit(50000)
    sessions_df.cache()
    print("Sessions data loaded. Running groupBy operation...")
    funnel_df = sessions_df.groupBy("conversion_status").count().toPandas()
    print(f"Conversion funnel query completed. DataFrame size: {len(funnel_df)} rows")
    if funnel_df.empty:
        print("Warning: Conversion funnel DataFrame is empty")
    else:
        plt.figure(figsize=(10, 6))
        sns.barplot(x="count", y="conversion_status", data=funnel_df)
        plt.title("Conversion Funnel Stages")
        try:
            plt.savefig("visualizations/conversion_funnel.png")
            print("Saved conversion funnel plot")
        except Exception as e:
            print(f"Failed to save conversion funnel plot: {e}")
        plt.close()

        print("Writing conversion funnel to HBase...")
        try:
            connection = happybase.Connection("localhost", port=9090, timeout=60000)
            table = connection.table("funnel_metrics")
            data = {row["conversion_status"]: {"metrics:count": str(row["count"])} for _, row in funnel_df.iterrows()}
            if write_to_hbase_with_retry(table, data):
                print("Wrote conversion funnel to HBase")
            else:
                print("Failed to write conversion funnel to HBase after retries")
            connection.close()
        except Exception as e:
            print(f"HBase write for conversion funnel failed: {e}")
except Exception as e:
    print(f"Conversion funnel query failed: {e}")

# Top products: Aggregates revenue for top 10 products by subtotal
try:
    print("Running top products query...")
    top_products = spark.sql("""
        WITH exploded_transactions AS (
            SELECT item.product_id, item.subtotal
            FROM transactions t
            LATERAL VIEW explode(t.items) AS item
        )
        SELECT t.product_id, SUM(t.subtotal) as total_revenue
        FROM exploded_transactions t
        GROUP BY t.product_id
        ORDER BY total_revenue DESC
        LIMIT 10
    """).toPandas()
    print(f"Top products query completed. DataFrame size: {len(top_products)} rows")
    if top_products.empty:
        print("Warning: Top products DataFrame is empty")
    else:
        plt.figure(figsize=(10, 6))
        sns.barplot(x="product_id", y="total_revenue", data=top_products, palette="viridis")
        plt.title("Top 10 Products by Revenue")
        plt.xlabel("Product ID")
        plt.ylabel("Total Revenue")
        plt.xticks(rotation=45, ha="right")  # Rotate x-axis labels for better readability
        plt.tight_layout()  # Adjust layout to prevent label cutoff
        try:
            plt.savefig("visualizations/top_products.png")
            print("Saved top products plot")
        except Exception as e:
            print(f"Failed to save top products plot: {e}")
        plt.close()

        print("Writing top products to HBase...")
        try:
            connection = happybase.Connection("localhost", port=9090, timeout=60000)
            table = connection.table("products_metrics")
            data = {row["product_id"]: {"revenue:total": str(row["total_revenue"])} for _, row in top_products.iterrows()}
            if write_to_hbase_with_retry(table, data):
                print("Wrote top products to HBase")
            else:
                print("Failed to write top products to HBase after retries")
            connection.close()
        except Exception as e:
            print(f"HBase write for top products failed: {e}")
except Exception as e:
    print(f"Top products query failed: {e}")

# Sales over time: Aggregates daily transaction totals
try:
    print("Running sales over time query...")
    sales_over_time = spark.sql("""
        SELECT DATE_TRUNC('day', CAST(t.timestamp AS TIMESTAMP)) AS day,
               SUM(t.total) AS total_revenue
        FROM transactions t
        GROUP BY DATE_TRUNC('day', CAST(t.timestamp AS TIMESTAMP))
        ORDER BY day
    """).toPandas()
    print(f"Sales over time query completed. DataFrame size: {len(sales_over_time)} rows")
    if sales_over_time.empty:
        print("Warning: Sales over time DataFrame is empty")
    else:
        plt.figure(figsize=(15, 6))
        sns.lineplot(x="day", y="total_revenue", data=sales_over_time)
        plt.title("Sales Performance Over Time")
        plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=7))
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        try:
            plt.savefig("visualizations/sales_over_time.png")
            print("Saved sales over time plot")
        except Exception as e:
            print(f"Failed to save sales over time plot: {e}")
        plt.close()

        print("Writing sales over time to HBase...")
        try:
            connection = happybase.Connection("localhost", port=9090, timeout=60000)
            table = connection.table("sales_over_time")
            data = {str(row["day"]): {"revenue:total": str(row["total_revenue"])} for _, row in sales_over_time.iterrows()}
            if write_to_hbase_with_retry(table, data):
                print("Wrote Ysales over time to HBase")
            else:
                print("Failed to write sales over time to HBase after retries")
            connection.close()
        except Exception as e:
            print(f"HBase write for sales over time failed: {e}")
except Exception as e:
    print(f"Sales over time query failed: {e}")

# MongoDB: Revenue by category (from query_mongodb.py)
try:
    print("Running MongoDB revenue by category query...")
    client = MongoClient("mongodb://localhost:27017")
    db = client["db_ecommerce"]
    pipeline = [
        {"$unwind": "$items"},
        {"$lookup": {
            "from": "products",
            "localField": "items.product_id",
            "foreignField": "product_id",
            "as": "product_info"
        }},
        {"$unwind": "$product_info"},
        {"$group": {
            "_id": "$product_info.category.category_id",
            "total_revenue": {"$sum": "$items.subtotal"}
        }},
        {"$sort": {"total_revenue": -1}}
    ]
    mongo_data = list(db.transactions.aggregate(pipeline))
    mongo_df = spark.createDataFrame(
        [(d["_id"], d["total_revenue"]) for d in mongo_data],
        ["category_id", "total_revenue"]
    ).toPandas()
    print(f"MongoDB revenue by category query completed. DataFrame size: {len(mongo_df)} rows")
    if mongo_df.empty:
        print("Warning: MongoDB revenue by category DataFrame is empty")
    else:
        plt.figure(figsize=(10, 6))
        sns.barplot(x="total_revenue", y="category_id", data=mongo_df)
        plt.title("Revenue by Category (MongoDB)")
        try:
            plt.savefig("visualizations/revenue_by_category_mongo.png")
            print("Saved MongoDB revenue by category plot")
        except Exception as e:
            print(f"Failed to save MongoDB revenue by category plot: {e}")
        plt.close()

        print("Writing MongoDB revenue by category to HBase...")
        try:
            connection = happybase.Connection("localhost", port=9090, timeout=60000)
            table = connection.table("sales_by_category_mongo")
            data = {row["category_id"]: {"revenue:total": str(row["total_revenue"])} for _, row in mongo_df.iterrows()}
            if write_to_hbase_with_retry(table, data):
                print("Wrote MongoDB revenue by category to HBase")
            else:
                print("Failed to write MongoDB revenue by category to HBase after retries")
            connection.close()
        except Exception as e:
            print(f"HBase write for MongoDB revenue by category failed: {e}")
except Exception as e:
    print(f"MongoDB revenue by category query failed: {e}")

# User Segmentation by Purchase Frequency
try:
    print("Running user segmentation query...")
    client = MongoClient("mongodb://localhost:27017")
    db = client["db_ecommerce"]
    pipeline = [
        {"$group": {
            "_id": "$summary.total_purchases",
            "user_count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    user_segment_data = list(db.users.aggregate(pipeline))
    segment_df = spark.createDataFrame(
        [(d["_id"], d["user_count"]) for d in user_segment_data],
        ["purchase_count", "user_count"]
    ).toPandas()
    print(f"User segmentation query completed. DataFrame size: {len(segment_df)} rows")
    if segment_df.empty:
        print("Warning: User segmentation DataFrame is empty")
    else:
        plt.figure(figsize=(10, 6))
        sns.barplot(x="user_count", y="purchase_count", data=segment_df, orient="h")
        plt.title("User Segmentation by Purchase Frequency")
        plt.ylabel("Number of Purchases")
        plt.xlabel("Number of Users")
        try:
            plt.savefig("final_project/visualizations/user_segmentation.png")
            print("Saved user segmentation plot")
        except Exception as e:
            print(f"Failed to save user segmentation plot: {e}")
        plt.close()

        print("Writing user segmentation to HBase...")
        try:
            connection = happybase.Connection("localhost", port=9090, timeout=60000)
            table = connection.table("user_segmentation")
            data = {str(row["purchase_count"]): {"metrics:user_count": str(row["user_count"])} for _, row in segment_df.iterrows()}
            if write_to_hbase_with_retry(table, data):
                print("Wrote user segmentation to HBase")
            else:
                print("Failed to write user segmentation to HBase after retries")
            connection.close()
        except Exception as e:
            print(f"HBase write for user segmentation failed: {e}")
except Exception as e:
    print(f"User segmentation query failed: {e}")

# Unpersist cached DataFrames
try:
    transactions_df.unpersist()
    products_df.unpersist()
    users_df.unpersist()
    print("Unpersisted cached DataFrames")
except Exception as e:
    print(f"Error unpersisting DataFrames: {e}")

# Stop Spark session
try:
    spark.stop()
    print("Spark session stopped")
except Exception as e:
    print(f"Error stopping Spark session: {e}")

print("Visualizations completed")