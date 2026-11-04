# Evidence screenshots

| File | What it shows | What it supports |
|---|---|---|
| `01_setup_machine_ram_docker.png` | Machine check: 8,056 MB RAM and Docker 29.8.1 | Environment used (8 GB laptop), explains why the dataset was reduced |
| `02_data_generation_summary.png` | Dataset generation finished: 200,000 sessions and 50,000 transactions | Dataset created with the lecturer's generator |
| `03_data_files_and_sizes.png` | Generated files and their sizes (about 400 MB, sessions are 94%) | Data volume; sessions dominate, so they go to HBase and Spark |
| `04_session_document_sample.png` | One real session document (nested page_views, cart_contents) | Nested structure that motivates the schema choices |
| `05_docker_pull_mongo.png` | MongoDB 7 image downloaded with Docker | MongoDB setup |
| `06_docker_ps_mongo_first_attempt_port27017.png` | First MongoDB container, mapped to port 27017 (before the port conflict was found) | Lesson learned: port 27017 was also used by a local MongoDB |
| `07_mongosh_connected_ping_ok.png` | MongoDB 7.0.45 answers ping ({ ok: 1 }) from the shell | MongoDB running |
| `08_mongo_loader_counts_first_run.png` | Loader output: 25 / 5,000 / 10,000 / 50,000 / 20,000 documents (first run, before the port fix) | Load counts; this run went to the local MongoDB, the Docker load was verified in figures 10 and 11 |
| `09_docker_ps_mongo_port27018.png` | MongoDB container recreated on port 27018 | Port conflict fixed |
| `10_mongo_transaction_embedded_items.png` | A transaction document with its items embedded (Docker MongoDB) | Embedded line items design |
| `11_mongo_user_computed_summary.png` | A user document with the computed purchase summary (Docker MongoDB) | Computed pattern: total_orders, total_spent, first/last purchase |
| `12_mongo_pipeline_q1_revenue_by_category.png` | Aggregation pipeline 1: revenue by category | Non-trivial aggregation ($unwind, $lookup, $group) |
| `13_mongo_user_summary_second_view.png` | User document with summary (second view, duplicate of figure 11) | Optional duplicate |
| `14_mongo_pipelines_q2_q3_index_demo.png` | Pipelines 2 and 3 (user segments, top products) and the index demonstration | Index: 4 documents examined instead of 50,000 |
| `15_docker_pull_hbase.png` | HBase 1.4 image downloaded (ports 9090 and 16010 checked free first) | HBase setup |
| `16_docker_ps_both_containers.png` | Both containers running: HBase and MongoDB | Both databases up |
| `17_hbase_shell_status.png` | HBase shell 1.4.7: 1 active master, 0 dead, 1 server | HBase healthy |
| `18_hbase_tables_created_list.png` | Tables user_sessions and product_daily created | HBase schema in place |
| `19_hbase_describe_user_sessions.png` | describe user_sessions: column families cart, info, pv | Column-family design |
| `20_hbase_loader_50000_rows.png` | HBase load: 50,000 sessions and 142,040 product_daily rows | Sessions loaded into HBase |
| `21_hbase_queries_q1_q4.png` | HBase queries Q1 to Q4: latest sessions, page views, time window, family pruning | Prefix scan, wide row, range scan |
| `22_hbase_queries_q4_q5.png` | HBase queries Q4 and Q5: 52 vs 94 cells; product metrics for September | Family pruning; product time series |
| `23_hbase_shell_prefix_scan.png` | HBase shell prefix scan for one user (info family only) | Shell query example |
| `24_hbase_shell_count_50000.png` | HBase shell count: 50,000 rows | Row count matches the loader |
| `25_spark_version_test.png` | Spark 3.5.9 running in local mode | Spark setup |
| `26_convert_to_jsonl.png` | Conversion of JSON arrays to JSON Lines (400,000+ records) | Optimization: splittable input for Spark |
| `27_spark_data_quality_report_1.png` | Data quality report, part 1 | Cleaning job output |
| `28_spark_data_quality_report_2.png` | Data quality report, part 2 | Quantified data problems |
| `29_spark_cleaned_schema_1.png` | Cleaned sessions schema, part 1 | Typed, normalized data |
| `30_spark_cleaned_schema_2.png` | Cleaned sessions schema, part 2 (flag columns) | Flag, don't delete |
| `31_spark_cleaned_sample.png` | Cleaned sessions sample | Cleaning result |
| `32_spark_recommend_chance_baseline.png` | Co-viewed products: observed pairs vs random baseline | Recommendation indicators with validation |
| `33_spark_recommend_category_affinity.png` | Category affinity (relative lift) and start of recommendations | No category pair beyond random noise |
| `34_spark_recommend_bestsellers.png` | Category best-seller recommendations | Fallback recommender |
| `35_spark_sql_q1_monthly_revenue.png` | Spark SQL Q1: monthly revenue with per-day growth | SQL with window functions |
| `36_spark_sql_q2_top_products.png` | Spark SQL Q2: top 3 products per category | RANK and SUM OVER |
| `37_spark_sql_q3_spend_quartiles.png` | Spark SQL Q3: spend quartiles vs engagement | MongoDB-style users joined with HBase-style sessions |
| `38_spark_sql_q4_inventory_risk.png` | Spark SQL Q4: active products already out of stock, ranked by demand | Three-way join |
| `39_spark_sql_q5_cohorts.png` | Spark SQL Q5: cohort analysis by registration month | Cohort matrix |
| `40_hbase_after_restart_q1_q4.png` | HBase queries after restarting both databases (same results) | Data persistence across restarts |
| `41_hbase_after_restart_q4_q5.png` | HBase queries after restart, continued | Persistence |
| `42_integrated_funnel_part1.png` | Integrated funnel (HBase + MongoDB + Spark): integrity check and overall funnel | Part 3 integrated query |
| `43_integrated_funnel_part2.png` | Funnel by referrer and device, abandoned carts, cross-check with all sessions | Part 3 results and validation |
| `44_make_charts_run.png` | Chart generation: five PNG charts saved | Part 4 visualizations |
| `45_spark_streaming_sim.png` | Simulated streaming run: 91 daily micro-batches, totals identical to the batch job | Learning objective: simulated stream processing |

