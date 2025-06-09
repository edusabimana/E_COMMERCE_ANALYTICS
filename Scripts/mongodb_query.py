from pymongo import MongoClient

# Establish MongoDB connection
client = MongoClient("mongodb://localhost:27017")
db = client["db_ecommerce"]

# Helper function to get database
def get_db():
    return client["db_ecommerce"]

# Query 1: Top-selling products
def top_selling_products():
    """Return top 10 selling products as a list."""
    pipeline = [
        {"$unwind": "$items"},
        {"$group": {
            "_id": "$items.product_id",
            "total_revenue": {"$sum": "$items.subtotal"},
            "total_quantity": {"$sum": "$items.quantity"}
        }},
        {"$sort": {"total_revenue": -1}},
        {"$limit": 10}
    ]
    print("Top-selling products:")
    for r in db.transactions.aggregate(pipeline):
        print(r)

# Query 2: Revenue by category
def revenue_by_category():
    """Return revenue by product category."""
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
    print("\nRevenue by category:")
    for r in db.transactions.aggregate(pipeline):
        print(r)


# Execute the queries
top_selling_products()
revenue_by_category()
