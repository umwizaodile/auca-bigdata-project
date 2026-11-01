"""
convert_to_jsonl.py
Converts the generator's JSON-array files into JSON Lines (one record per line)
so that Spark can split them into partitions and process them in parallel.
Output goes to the data/ folder.
"""
import json
import os
import sys

FILES = [
    ("categories.json",   "data/categories.jsonl"),
    ("products.json",     "data/products.jsonl"),
    ("users.json",        "data/users.jsonl"),
    ("transactions.json", "data/transactions.jsonl"),
    ("sessions_0.json",   "data/sessions_0.jsonl"),
    ("sessions_1.json",   "data/sessions_1.jsonl"),
]


def convert(src, dst):
    """Read one JSON-array file and write it back as one JSON record per line."""
    if not os.path.exists(src):
        sys.exit(f"ERROR: {src} not found. Run dataset_generator.py first.")
    with open(src, encoding="utf-8") as f:
        records = json.load(f)
    with open(dst, "w", encoding="utf-8") as out:
        for record in records:
            out.write(json.dumps(record, separators=(",", ":")) + "\n")
    size_mb = os.path.getsize(dst) / 1_000_000
    print(f"{src:<20} -> {dst:<26} {len(records):>9,} records  {size_mb:8.1f} MB")


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    for src, dst in FILES:
        convert(src, dst)
    print("Done.")