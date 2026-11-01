"""
mongo_queries.py
Three MongoDB aggregation pipelines for the e-commerce analytics project.
Results are printed and saved to the results/ folder as CSV (used for charts later).

  Q1  Revenue analytics      : revenue by category
  Q2  User segmentation      : users grouped by purchasing frequency
  Q3  Product popularity     : top 10 best-selling products
Plus an index demonstration (explain plan) for the performance discussion.
"""
import os
import sys

import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

client = MongoClient("mongodb://127.0.0.1:27018", serverSelectionTimeoutMS=3000)
try:
    client.admin.command("ping")
except ServerSelectionTimeoutError:
    sys.exit("ERROR: cannot reach MongoDB. Is the 'mongo-auca' container running? (docker ps)")
db = client["ecommerce"]
os.makedirs("results", exist_ok=True)


def run(title, collection, pipeline, csv_name):
    """Run a pipeline, print the result as a table and save it as CSV."""
    rows = list(db[collection].aggregate(pipeline, allowDiskUse=True))
    df = pd.DataFrame(rows)
    df.to_csv(os.path.join("results", csv_name), index=False)
    print(f"\n=== {title} ===")
    print(df.to_string(index=False))
    return df


# ---------------------------------------------------------------------------
# Q1. Revenue by category
# Business question: which product categories generate the most revenue?
# Stages: $unwind items -> $lookup product -> $group by category -> $project -> $sort
# ---------------------------------------------------------------------------
revenue_by_category = [
    {"$unwind": "$items"},                       # one document per line item
    {"$lookup": {                                # join each item to its product
        "from": "products",
        "localField": "items.product_id",
        "foreignField": "product_id",
        "as": "product"}},
    {"$unwind": "$product"},
    {"$group": {
        "_id": {"category_id": "$product.category_id",
                "category_name": "$product.category_name"},
        "revenue": {"$sum": "$items.subtotal"},
        "units_sold": {"$sum": "$items.quantity"},
        "orders": {"$addToSet": "$transaction_id"}}},
    {"$project": {
        "_id": 0,
        "category_id": "$_id.category_id",
        "category_name": "$_id.category_name",
        "revenue": {"$round": ["$revenue", 2]},
        "units_sold": 1,
        "orders": {"$size": "$orders"},
        "avg_revenue_per_order": {"$round": [
            {"$divide": ["$revenue", {"$size": "$orders"}]}, 2]}}},
    {"$sort": {"revenue": -1}},
]

# ---------------------------------------------------------------------------
# Q2. User segmentation by purchasing frequency
# Business question: how are customers split by how often they buy,
# and how much does each segment contribute to revenue?
# Uses the precomputed users.summary (computed pattern) -> no re-scan of transactions.
# ---------------------------------------------------------------------------
user_segments = [
    {"$addFields": {"segment": {"$switch": {
        "branches": [
            {"case": {"$eq": ["$summary.total_orders", 0]}, "then": "1. No purchase"},
            {"case": {"$eq": ["$summary.total_orders", 1]}, "then": "2. One-time buyer"},
            {"case": {"$lte": ["$summary.total_orders", 4]}, "then": "3. Occasional (2-4)"},
            {"case": {"$lte": ["$summary.total_orders", 9]}, "then": "4. Regular (5-9)"},
        ],
        "default": "5. Loyal (10+)"}}}},
    {"$group": {
        "_id": "$segment",
        "users": {"$sum": 1},
        "total_revenue": {"$sum": "$summary.total_spent"},
        "avg_spent_per_user": {"$avg": "$summary.total_spent"},
        "avg_orders": {"$avg": "$summary.total_orders"}}},
    {"$project": {
        "_id": 0,
        "segment": "$_id",
        "users": 1,
        "total_revenue": {"$round": ["$total_revenue", 2]},
        "avg_spent_per_user": {"$round": ["$avg_spent_per_user", 2]},
        "avg_orders": {"$round": ["$avg_orders", 1]}}},
    {"$sort": {"segment": 1}},
]

# ---------------------------------------------------------------------------
# Q3. Top 10 best-selling products
# Business question: which products sell the most units, and what revenue do they bring?
# $limit runs BEFORE $lookup, so only 10 joins are performed (optimization).
# ---------------------------------------------------------------------------
top_products = [
    {"$unwind": "$items"},
    {"$group": {
        "_id": "$items.product_id",
        "units_sold": {"$sum": "$items.quantity"},
        "revenue": {"$sum": "$items.subtotal"}}},
    {"$sort": {"units_sold": -1, "revenue": -1}},
    {"$limit": 10},
    {"$lookup": {
        "from": "products",
        "localField": "_id",
        "foreignField": "product_id",
        "as": "product"}},
    {"$unwind": "$product"},
    {"$project": {
        "_id": 0,
        "product_id": "$_id",
        "name": "$product.name",
        "category_name": "$product.category_name",
        "units_sold": 1,
        "revenue": {"$round": ["$revenue", 2]}}},
]

if __name__ == "__main__":
    run("Q1 Revenue by category", "transactions", revenue_by_category, "revenue_by_category.csv")
    run("Q2 User segments by purchasing frequency", "users", user_segments, "user_segments.csv")
    run("Q3 Top 10 best-selling products", "transactions", top_products, "top_products.csv")

    # --- Index demonstration: same query with and without the index ---------
    print("\n=== Index demonstration: transactions for one user ===")
    query = {"user_id": "user_000042"}
    with_index = db.transactions.find(query).explain()["executionStats"]
    no_index = db.transactions.find(query).hint([("$natural", 1)]).explain()["executionStats"]
    print(f"With index    : examined {with_index['totalDocsExamined']:,} docs, "
          f"returned {with_index['nReturned']}, {with_index['executionTimeMillis']} ms")
    print(f"Full scan     : examined {no_index['totalDocsExamined']:,} docs, "
          f"returned {no_index['nReturned']}, {no_index['executionTimeMillis']} ms")