# Distributed Multi-Model Analytics for E-commerce Data

AUCA Big Data Analytics, Final Project (October 2026)
Author: ODILE UMWIZA, Student ID: 101365

An analytics system for a synthetic e-commerce dataset that uses each technology for what it does best:
**MongoDB** (document model) for products, users and transactions, **HBase** (wide-column) for browsing
sessions and product time series, and **Apache Spark** for cleaning, recommendations, SQL analytics,
simulated stream processing and the cross-system funnel analysis.

## Architecture

```
dataset_generator.py --> JSON files
    |-- convert_to_jsonl.py --> data/*.jsonl --> Spark: spark_clean, spark_recommend, spark_sql, spark_streaming_sim
    |-- load_mongodb.py --> MongoDB (products, users, transactions, 20k sessions) --> mongo_queries.py, mongo_session_queries.py
    '-- load_hbase.py  --> HBase (50k sessions, product_daily)                    --> hbase_queries.py
MongoDB + HBase --> spark_integrated.py (funnel) --> results/*.csv --> make_charts.py --> charts/*.png
```

| Data | Store | Why |
|---|---|---|
| Transactions with embedded items, products with price history, users with a computed summary | MongoDB | Read together, bounded size, flexible aggregation pipelines |
| Sessions (about 94% of the volume) and product daily metrics | HBase | Write-once event data read by user and time; key design gives fast range scans |
| Cleaning, recommendations, SQL, streaming replay, cross-system joins | Spark | Distributed processing; same code scales from a laptop to a cluster |

## Technologies

Python 3.11, MongoDB 7 (Docker), HBase 1.4.7 (Docker image `harisekhon/hbase:1.4`), PySpark 3.5.9,
Java 8 to 17 (needed by Spark), PyMongo, HappyBase, pandas, Matplotlib. Tested on Windows
(PowerShell) with 8 GB RAM.

## Repository contents

| File | Purpose |
|---|---|
| `dataset_generator.py` | Lecturer's generator (extracted from the notebook); only `NUM_TRANSACTIONS` (50,000) and `NUM_SESSIONS` (200,000) were reduced |
| `convert_to_jsonl.py` | Converts the JSON arrays to JSON Lines so Spark can split them into partitions |
| `load_mongodb.py` | Loads MongoDB, converts dates, adds the user summary, creates indexes |
| `mongo_queries.py` | Three aggregation pipelines (revenue, segments, top products) and an index demonstration |
| `mongo_session_queries.py` | Two pipelines on the nested session documents (most viewed products, journey by referrer) |
| `hbase_schema.hbase`, `hbase_shell_queries.hbase` | HBase schema creation and shell query examples |
| `load_hbase.py` | Loads 50,000 sessions into HBase and builds `product_daily` |
| `hbase_queries.py` | Five HBase query patterns with the Python client |
| `spark_common.py` | Explicit schemas and the shared cleaning functions |
| `spark_clean.py` | Spark batch job 1: cleaning and data quality report |
| `spark_recommend.py` | Spark batch job 2: recommendation indicators with a chance baseline |
| `spark_sql.py` | Spark SQL analytics (5 queries on `mongo_*` and `hbase_*` views) |
| `spark_streaming_sim.py` | Simulated stream processing: 91 daily micro-batches, running state, alerts, check against batch totals |
| `spark_integrated.py` | Part 3: funnel analysis joining HBase and MongoDB data in Spark |
| `make_charts.py` | Builds five charts from the saved results (the streaming chart is built by `spark_streaming_sim.py`) |
| `report/` | Technical report (PDF) |
| `results/`, `charts/`, `screenshots/` | Output tables, figures and evidence screenshots |

## Setup and reproduction

Requirements: Python 3.10+, Docker Desktop, Java 8 to 17, about 8 GB RAM.

**1. Environment**

```
python -m venv venv
venv\Scripts\activate          # Windows PowerShell (Linux/macOS: source venv/bin/activate)
pip install -r requirements.txt
```

**2. Generate and convert the data** (a few minutes; about 400 MB of files, not committed to Git)

```
python dataset_generator.py
python convert_to_jsonl.py
```

**3. Start the databases.** MongoDB is mapped to port 27018 because 27017 is often used by a local MongoDB install.

```
docker run -d --name mongo-auca -p 27018:27017 -v mongo_data:/data/db mongo:7
docker run -d --name hbase-auca -h hbase-auca -p 9090:9090 -p 16010:16010 -v hbase_data:/hbase-data harisekhon/hbase:1.4
```

Wait about 2 minutes for HBase, then create its tables (Windows PowerShell shown; Linux/macOS:
`docker exec -i hbase-auca hbase shell < hbase_schema.hbase`):

```
Get-Content hbase_schema.hbase | docker exec -i hbase-auca hbase shell
```

**4. Load the databases**

```
python load_mongodb.py       # expect 25 / 5,000 / 10,000 / 50,000 / 20,000 documents
python load_hbase.py         # expect 50,000 rows in user_sessions
```

**5. Run the analyses** (Spark jobs take roughly 3 to 10 minutes each; with 8 GB RAM, stop the
databases for the Spark-only jobs with `docker stop mongo-auca hbase-auca`)

| Command | Needs | Output |
|---|---|---|
| `python mongo_queries.py` | MongoDB | `results/revenue_by_category.csv`, `user_segments.csv`, `top_products.csv` |
| `python mongo_session_queries.py` | MongoDB | `results/most_viewed_products.csv`, `session_journey_by_referrer.csv` |
| `python hbase_queries.py` | HBase | printed query results |
| `python spark_clean.py` | JSONL files | `results/data_quality_report.csv` |
| `python spark_recommend.py` | JSONL files | `results/chance_baseline.csv`, `category_affinity.csv`, `category_bestsellers.csv` |
| `python spark_sql.py` | JSONL files | `results/sql_*.csv` |
| `python spark_streaming_sim.py` | JSONL files | `results/stream_batches.csv`, `charts/06_streaming_conversion.png` |
| `python spark_integrated.py` | MongoDB, HBase, JSONL | `results/funnel_*.csv` |
| `python make_charts.py` | results files | `charts/01` to `05` `*.png` |

HBase shell examples: `Get-Content hbase_shell_queries.hbase | docker exec -i hbase-auca hbase shell`.

## Key results

- **MongoDB:** an index on `transactions.user_id` examines 4 documents instead of 50,000 for one user's orders.
  Total revenue computed in MongoDB and in Spark SQL agrees exactly (53,926,975.67).
- **Revenue:** spread evenly across the 25 categories (the best earns 1.44x the weakest).
  Regular buyers (5 to 9 orders) are 52.7% of users and 66.3% of revenue.
- **Product views (MongoDB, 20,000 sessions):** the 10 most viewed products have 26 to 27 views, in line with
  chance (average 14.4 views per product); journeys and conversion are the same across traffic sources.
- **HBase:** reading only the `info` column family returns 52 cells instead of 94 for the same rows.
- **Data quality (200,000 sessions):** 21.8% visit the cart page with an empty cart, 7.8% are marked
  "converted" without a transaction, 49.8% of transactions have no session link, and country codes and
  `last_active` are unreliable.
- **Recommendations:** co-viewed product pairs match a random baseline and no category pair stands out
  beyond random noise, as expected for synthetic data. A category best-seller fallback is provided.
- **Funnel (50,000 HBase sessions + MongoDB purchases):** 20.3% of sessions end in a purchase. The biggest
  leak is cart to checkout (only 44.9% continue). No referrer or device differs beyond random noise.
  Abandoned carts are worth 17.9M against 7.9M of purchased revenue; recovering 10% would add about 22.5%
  (a scenario, not a forecast).
- **Stream simulation:** 200,000 sessions replayed as 91 daily micro-batches (about 2,200 sessions each);
  the running conversion rate ends at 20.3% (40,609 conversions) and the streaming totals are identical to
  the batch totals. One alert was raised in 91 batches, which is about what random variation predicts.

## Limitations and future work

- The data is synthetic and largely random, so behavioural patterns (affinity, cohort effects, channel
  differences) are weak by construction. The pipelines would find real patterns in real data.
- Dataset reduced to 200,000 sessions and 50,000 transactions for an 8 GB laptop; the code is unchanged for larger sizes.
- The generator stops creating transactions at its 50,000 quota, so purchase records are incomplete for
  later sessions. The funnel therefore uses the first sessions, and a cross-check validates it.
- `product_daily` is built from the 50,000 loaded sessions. Everything runs on a single node without authentication.
- Stream processing is simulated by replaying stored sessions in time order (no live source, perfectly ordered
  events, no late data). Production would use Kafka with Spark Structured Streaming, watermarks and checkpoints.
- Native Python clients are used for the cross-system query; production would use the MongoDB and HBase Spark connectors.
- Cleaned data is not written as Parquet (`WRITE_PARQUET = False` in `spark_clean.py`) because writing
  files from Spark on Windows needs extra Hadoop helper files.

## Screenshots and report

Evidence of each step (data generation, containers, loads, queries, Spark runs, charts) is in `screenshots/`,
with a captioned Word version in `screenshots/evidence_screenshots.docx`. The technical report is in `report/`.