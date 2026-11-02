"""
mongo_session_queries.py
Two aggregation pipelines on the SESSION documents stored in MongoDB (20,000-session sample).
They show the document model on nested session data: one document holds the whole journey
(referrer, device, page_views array, cart, outcome).

  Q4  Most viewed products   ($unwind the page_views array, filter product pages, group)
  Q5  Session journey by referrer (average journey length and conversion per traffic source)
Results are printed and saved to results/ as CSV.
"""
import os
import sys

import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

MONGO_URI = "mongodb://127.0.0.1:27018"


def connect():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    try:
        client.admin.command("ping")
    except ServerSelectionTimeoutError:
        sys.exit("ERROR: cannot reach MongoDB. Is the 'mongo-auca' container running? (docker ps)")
    return client["ecommerce"]


# Q4. Business question: which products attract the most attention?
most_viewed = [
    {"$unwind": "$page_views"},                                   # one document per page view
    {"$match": {"page_views.page_type": "product_detail",
                "page_views.product_id": {"$ne": None}}},
    {"$group": {"_id": "$page_views.product_id",
                "views": {"$sum": 1},
                "sessions": {"$addToSet": "$session_id"},
                "view_seconds": {"$sum": "$page_views.view_duration"}}},
    {"$project": {"views": 1,
                  "unique_sessions": {"$size": "$sessions"},
                  "avg_view_seconds": {"$round": [{"$divide": ["$view_seconds", "$views"]}, 1]}}},
    {"$sort": {"views": -1, "_id": 1}},
    {"$limit": 10},                                               # limit BEFORE the join
    {"$lookup": {"from": "products", "localField": "_id",
                 "foreignField": "product_id", "as": "product"}},
    {"$unwind": "$product"},
    {"$project": {"_id": 0, "product_id": "$_id", "name": "$product.name",
                  "category_name": "$product.category_name", "views": 1,
                  "unique_sessions": 1, "avg_view_seconds": 1}},
]

# Q5. Business question: does journey length or conversion differ by traffic source?
journey_by_referrer = [
    {"$group": {"_id": "$referrer",
                "sessions": {"$sum": 1},
                "avg_page_views": {"$avg": {"$size": "$page_views"}},
                "avg_duration_s": {"$avg": "$duration_seconds"},
                "with_cart": {"$sum": {"$cond": [
                    {"$in": ["$conversion_status", ["converted", "abandoned"]]}, 1, 0]}},
                "converted": {"$sum": {"$cond": [
                    {"$eq": ["$conversion_status", "converted"]}, 1, 0]}}}},
    {"$project": {"_id": 0, "referrer": "$_id", "sessions": 1,
                  "avg_page_views": {"$round": ["$avg_page_views", 1]},
                  "avg_duration_s": {"$round": ["$avg_duration_s", 0]},
                  "with_cart": 1, "converted": 1,
                  "conversion_rate_pct": {"$round": [
                      {"$multiply": [{"$divide": ["$converted", "$sessions"]}, 100]}, 1]}}},
    {"$sort": {"conversion_rate_pct": -1}},
]


def run(db, title, pipeline, csv_name):
    df = pd.DataFrame(list(db.sessions.aggregate(pipeline, allowDiskUse=True)))
    os.makedirs("results", exist_ok=True)
    df.to_csv(os.path.join("results", csv_name), index=False)
    print(f"\n=== {title} ===")
    print(df.to_string(index=False))
    return df


if __name__ == "__main__":
    db = connect()
    n = db.sessions.count_documents({})
    print(f"Sessions in MongoDB: {n:,}")
    run(db, "Q4 Top 10 most viewed products", most_viewed, "most_viewed_products.csv")
    run(db, "Q5 Session journey by referrer", journey_by_referrer, "session_journey_by_referrer.csv")