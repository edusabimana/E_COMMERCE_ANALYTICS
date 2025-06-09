from pymongo import MongoClient
import json

client = MongoClient("mongodb://localhost:27017")
db = client["db_ecommerce"]

# Load users
with open("data/users.json") as f:
    users = json.load(f)
    for user in users:
        user["summary"] = {"total_purchases": 0, "avg_spend": 0.0}
    db.users.insert_many(users[:1000])  # Subset

# Load categories and products
with open("data/categories.json") as f:
    categories = json.load(f)
category_map = {c["category_id"]: c for c in categories}

with open("data/products.json") as f:
    products = json.load(f)
    for product in products:
        cat_id = product["category_id"]
        category = category_map.get(cat_id, {})
        subcat = next((s for s in category.get("subcategories", []) if s["subcategory_id"] == f"sub_{cat_id[-3:]}_01"), {})
        product["category"] = {
            "category_id": cat_id,
            "name": category.get("name"),
            "subcategory_id": subcat.get("subcategory_id"),
            "subcategory_name": subcat.get("name")
        }
    db.products.insert_many(products[:1000])  # Subset

# Load transactions
with open("data/transactions.json") as f:
    transactions = json.load(f)
    db.transactions.insert_many(transactions[:10000])  # Subset

# Populate user summary with purchase statistics
print("Populating user purchase statistics...")
transactions = db.transactions.find()
user_purchases = {}
for txn in transactions:
    user_id = txn["user_id"]
    total = txn["total"]
    user_purchases[user_id] = user_purchases.get(user_id, {"count": 0, "total": 0})
    user_purchases[user_id]["count"] += 1
    user_purchases[user_id]["total"] += total

for user_id, stats in user_purchases.items():
    avg_spend = stats["total"] / stats["count"] if stats["count"] > 0 else 0
    db.users.update_one(
        {"user_id": user_id},
        {"$set": {"summary.total_purchases": stats["count"], "summary.avg_spend": avg_spend}}
    )

print("User purchase statistics populated.")
client.close()