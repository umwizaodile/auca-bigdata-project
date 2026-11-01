"""
load_mongodb.py
Loads the generated e-commerce data into MongoDB using the document model
designed for this project.

Collections created (database: ecommerce):
  categories   - category + embedded subcategories
  products     - product + embedded price_history + denormalized category_name
  users        - profile + embedded geo_data + computed purchase summary
  transactions - transaction + embedded items
  sessions     - SAMPLE of browsing sessions (the full set goes to HBase/Spark)
"""
import json
import os
import sys
from collections import defaultdict
from datetime import datetime

from pymongo import ASCENDING, MongoClient
from pymongo.errors import ServerSelectionTimeoutError

MONGO_URI = "mongodb://127.0.0.1:27018"
DB_NAME = "ecommerce"
SESSION_SAMPLE_SIZE = 20000   # sessions kept in MongoDB (rest: HBase / Spark)
BATCH_SIZE = 5000             # documents per insert_many call


def to_dt(value):
    """Convert an ISO date string to a datetime object (None if missing/invalid)."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None


def load_json(path):
    """Read a JSON file, stopping with a clear message if it does not exist."""
    if not os.path.exists(path):
        sys.exit(f"ERROR: {path} not found. Run dataset_generator.py first.")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def insert_in_batches(collection, docs):
    """Insert documents in chunks so memory use and request size stay small."""
    for i in range(0, len(docs), BATCH_SIZE):
        collection.insert_many(docs[i:i + BATCH_SIZE])
    print(f"  {collection.name}: {len(docs):,} documents inserted")


def main():
    # --- Connect and verify MongoDB is reachable -------------------------
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    try:
        client.admin.command("ping")
    except ServerSelectionTimeoutError:
        sys.exit("ERROR: cannot reach MongoDB. Is the 'mongo-auca' container running? (docker ps)")
    db = client[DB_NAME]
    print("Connected to MongoDB.")

    # Drop old collections so the script can be re-run safely
    for name in ["categories", "products", "users", "transactions", "sessions"]:
        db[name].drop()

    # --- Categories -------------------------------------------------------
    categories = load_json("categories.json")
    insert_in_batches(db.categories, categories)
    category_names = {c["category_id"]: c["name"] for c in categories}

    # --- Products: real dates + denormalized category name ----------------
    products = load_json("products.json")
    for p in products:
        p["creation_date"] = to_dt(p["creation_date"])
        for ph in p["price_history"]:
            ph["date"] = to_dt(ph["date"])
        p["category_name"] = category_names.get(p["category_id"])
    insert_in_batches(db.products, products)

    # --- Transactions: real dates + item_count ----------------------------
    transactions = load_json("transactions.json")
    summary = defaultdict(lambda: {"total_orders": 0, "total_spent": 0.0,
                                   "first_purchase": None, "last_purchase": None})
    for t in transactions:
        t["timestamp"] = to_dt(t["timestamp"])
        t["item_count"] = sum(item["quantity"] for item in t["items"])
        s = summary[t["user_id"]]
        s["total_orders"] += 1
        s["total_spent"] += t["total"]
        if s["first_purchase"] is None or t["timestamp"] < s["first_purchase"]:
            s["first_purchase"] = t["timestamp"]
        if s["last_purchase"] is None or t["timestamp"] > s["last_purchase"]:
            s["last_purchase"] = t["timestamp"]
    insert_in_batches(db.transactions, transactions)

    # --- Users: real dates + computed purchase summary --------------------
    users = load_json("users.json")
    empty = {"total_orders": 0, "total_spent": 0.0,
             "first_purchase": None, "last_purchase": None}
    for u in users:
        u["registration_date"] = to_dt(u["registration_date"])
        u["last_active"] = to_dt(u["last_active"])
        s = dict(summary.get(u["user_id"], empty))
        s["total_spent"] = round(s["total_spent"], 2)
        u["summary"] = s
    insert_in_batches(db.users, users)

    # --- Sessions: sample only -------------------------------------------
    print("Loading session sample (this reads a 188 MB file, please wait)...")
    sessions = load_json("sessions_0.json")[:SESSION_SAMPLE_SIZE]
    for s in sessions:
        s["start_time"] = to_dt(s["start_time"])
        s["end_time"] = to_dt(s["end_time"])
        for pv in s["page_views"]:
            pv["timestamp"] = to_dt(pv["timestamp"])
    insert_in_batches(db.sessions, sessions)

    # --- Indexes (make queries fast) -------------------------------------
    print("Creating indexes...")
    db.categories.create_index([("category_id", ASCENDING)], unique=True)
    db.products.create_index([("product_id", ASCENDING)], unique=True)
    db.products.create_index([("category_id", ASCENDING)])
    db.users.create_index([("user_id", ASCENDING)], unique=True)
    db.transactions.create_index([("transaction_id", ASCENDING)], unique=True)
    db.transactions.create_index([("user_id", ASCENDING)])
    db.transactions.create_index([("timestamp", ASCENDING)])
    db.transactions.create_index([("items.product_id", ASCENDING)])
    db.sessions.create_index([("session_id", ASCENDING)], unique=True)
    db.sessions.create_index([("user_id", ASCENDING)])
    db.sessions.create_index([("viewed_products", ASCENDING)])

    # --- Verification ----------------------------------------------------
    print("\nDocument counts:")
    for name in ["categories", "products", "users", "transactions", "sessions"]:
        print(f"  {name}: {db[name].count_documents({}):,}")
    print("\nDone.")


if __name__ == "__main__":
    main()