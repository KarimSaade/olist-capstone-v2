# Final presentation plan (8 to 10 minutes)

One storyline holds the talk together: **"Late deliveries cost reviews. I built the stack that finds them, measures them and flags them as they happen."**

| # | Slide | Time | What to show and say |
|---|---|---|---|
| 1 | Title and the problem | 0:30 | Olist marketplace, ~100k orders. The question: where does delivery experience break down, and can we see it in real time? NYC Taxi (4.67M rows) for the large-scale processing task. |
| 2 | Architecture | 1:00 | `docs/architecture.md` diagram. Batch path (CSV to Postgres via Airflow), streaming path (producer to Kafka to consumer), analytics (SQL, Spark). Everything in Docker. |
| 3 | SQL findings | 1:30 | Q4 chart: 4.3 stars early vs 1.7 when 11+ days late. Q2: Black Friday 2017 was 4.9x a normal day. One line each on Q1 (freight 20 to 24% in home categories) and Q3 (RJ top 3 sellers hold 38%). |
| 4 | SQL optimisation | 1:00 | Before/after table: Seq Scan to Index Only Scan, 1,840 to 74 pages, query about 38% faster. Mention the failed first attempt (plain index, scattered pages): it shows understanding, not just a result. |
| 5 | MongoDB design | 0:45 | Embedded product documents, referenced events with snapshot fields, CAP: majority writes for stock, relaxed for clicks. |
| 6 | PySpark processing and KPIs | 1:30 | Cleaning table (97.5% kept, 387k imputed rather than dropped). KPIs: March demand +41%, peak at 3pm at half the night-time speed, airports 3.6% of trips but 9.8% of revenue. |
| 7 | Spark performance and DAG | 1:00 | Pruning table: 84 MiB to 0.9 MiB, 2.5x faster. Spark UI screenshot: 3 tasks because gzip is not splittable. Honest trade-off: 90 tiny partitions made it slower when listing was included. |
| 8 | Docker, Airflow, Kafka live evidence | 1:30 | `docker compose ps`, green four-task DAG, Kafdrop topic with 3 partitions and zero consumer lag. Airflow mart shows RJ late rate 26% in Nov 2017, which links back to slide 3. |
| 9 | Sustainability and GDPR | 0:45 | No customer IDs downstream, secrets in `.env`, region choice for LGPD/GDPR, pruning as an energy saving. Gap: no TLS yet. |
| 10 | Limitations and next steps | 0:30 | Limitations: Spark in local mode, PLAINTEXT Kafka, Airflow re-reads full CSVs daily. Improvements: incremental loads, a Spark job in the DAG, Kafka sink into Postgres, TLS and SASL. |

## Questions to prepare for

* **Why not just index every column?** Each index slows writes and uses space. Q1, Q3 and Q4 scan most of the table, so a sequential scan is already optimal there.
* **Why did the whole query only get ~38% faster when `orders` access improved ~90%?** The remaining cost is the `order_items` scan and the sort; PostgreSQL rightly prefers one pass over 7.5k index lookups.
* **What is the difference between `PushedFilters` and `PartitionFilters`?** Pushed filters check row-group statistics inside files; partition filters skip whole folders before opening anything.
* **Why key Kafka messages by `order_id`?** Same key, same partition, so an order's events stay in order.
* **What happens if the consumer stops?** Offsets are committed in the `delivery-monitor` group; messages stay in the log and the consumer resumes where it left off (lag grows in Kafdrop meanwhile).
* **Why is the DAG idempotent?** `ON CONFLICT ... DO UPDATE` on the (state, month) key, so re-running replaces rows instead of duplicating them.
* **Why impute rather than drop the NULL passenger rows?** They are valid paid trips; dropping 7% of rows would understate revenue.
* **Is MongoDB CP or AP?** With a single primary and majority write concern it behaves as CP, but read preference and write concern let you trade consistency for latency per operation.
