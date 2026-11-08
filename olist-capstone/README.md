# Olist \& NYC Taxi: Big Data Capstone Portfolio

Big Data and Cloud-based AI Systems, final portfolio.

This repository brings together relational SQL, document-database design, distributed processing with PySpark, and a containerised pipeline with Airflow and Kafka. Most of it is built around one business: **Olist**, a Brazilian online marketplace (about 100k orders, 2016 to 2018). The PySpark task uses the **NYC Yellow Taxi** data instead, because it needs at least 500k rows and Olist's largest table has 112k.

The thread running through the work is *delivery experience*: SQL shows that late orders average 1.7 to 3.3 stars against 4.3 for early ones, the Airflow mart tracks late-delivery rates by state and month, and the Kafka consumer flags late deliveries as they happen.

## Repository layout

```
.
├── README.md
├── docker-compose.yml            Postgres, Airflow, Zookeeper, Kafka, Kafdrop
├── .env.example                  template for .env (git-ignored local secrets)
├── requirements.txt              Python packages for the notebooks and Kafka scripts
├── data/
│   ├── olist/                    7 Olist CSVs (download, see below)
│   ├── nyc\_taxi/                 downloaded automatically by the Spark notebook
│   └── processed/                Parquet written by the Spark notebook
├── task1\_sql\_nosql/
│   ├── 01\_olist\_sql\_analysis.ipynb   4 queries + EXPLAIN ANALYSE before/after index
│   ├── schema.sql                    tables, keys and types
│   ├── load\_olist.py                 bulk loads the CSVs with COPY
│   ├── nosql\_design\_brief.md / .pdf  MongoDB design brief
├── task2\_pyspark/
│   ├── 02\_nyc\_taxi\_pyspark.ipynb     cleaning, transformations, KPIs, partition pruning, DAG
│   └── spark\_justification.md
├── task3\_pipeline/
│   ├── airflow/dags/olist\_delivery\_etl.py   extract >> validate >> transform >> load
│   ├── kafka/producer.py                    replays Olist orders as live events
│   ├── kafka/consumer.py                    counts, filters late deliveries, flags big orders
│   └── postgres/init.sql                    creates the olist database
├── docs/
│   ├── architecture.md                      Mermaid diagram and component roles
│   ├── sustainability\_gdpr\_reflection.md / .pdf
└── evidence/                                screenshots referenced in the notebooks
```

## Requirements

Only **Docker Desktop** is needed (Windows 10/11 or macOS, at least 8 GB RAM, about 15 GB free disk). Python, Java, Spark, PostgreSQL, Airflow and Kafka all run inside containers defined in `docker-compose.yml`:

|Container|Image|Purpose|
|-|-|-|
|capstone-postgres|postgres:16|`olist` database (Task 1 and ETL target) and Airflow metadata|
|capstone-jupyter|quay.io/jupyter/pyspark-notebook|JupyterLab with Python, Java and PySpark; runs both notebooks and the Kafka scripts|
|capstone-airflow-webserver / -scheduler / -init|apache/airflow:2.9.3|ETL orchestration|
|capstone-zookeeper, capstone-kafka|confluentinc 7.4.0|event streaming|
|capstone-kafdrop|obsidiandynamics/kafdrop|Kafka web UI|

Python packages used inside the containers are listed in `requirements.txt`.

## Data sources

* **Olist Brazilian E-Commerce Public Dataset** (Kaggle, CC BY-NC-SA 4.0): https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce. The seven CSVs go in `data/olist/`.
* **NYC TLC Yellow Taxi trip records, Jan to Mar 2021** (CSV mirror by DataTalksClub on GitHub) and the taxi zone lookup. The Spark notebook downloads these itself (about 85 MB).

Raw data and `.env` are git-ignored.

## Setup

1. Create a `.env` file in the repo root from `.env.example` (passwords, Fernet key, Jupyter token).
2. From the repo root:

```bash
   docker compose up airflow-init        # one-off: creates the Airflow database and admin user
   docker compose up -d                  # starts everything in the background
   docker compose ps                     # all services should show "Up" / "healthy"
   ```

|Service|URL|Login|
|-|-|-|
|JupyterLab|http://localhost:8888|token = `JUPYTER\_TOKEN` in `.env`|
|Airflow|http://localhost:8080|`AIRFLOW\_ADMIN\_USER` / `AIRFLOW\_ADMIN\_PASSWORD`|
|Kafdrop|http://localhost:9000|none|
|Spark UI|http://localhost:4040|only while the Spark notebook is running|

On a laptop with 8 GB RAM, run Tasks 1 and 2 with only `docker compose up -d postgres jupyter`, then stop Jupyter (`docker compose stop jupyter`) before starting the full stack for Task 3.

## Execution order

1. **Task 1A, SQL.** In JupyterLab open `task1\_sql\_nosql/01\_olist\_sql\_analysis.ipynb` and choose *Run > Run All Cells*. It recreates the schema and loads the CSVs itself (`OLIST\_PG\_URL` is already set inside the container).
2. **Task 1B, NoSQL.** `task1\_sql\_nosql/nosql\_design\_brief.pdf`.
3. **Task 2, PySpark.** Open `task2\_pyspark/02\_nyc\_taxi\_pyspark.ipynb`, *Run All Cells* (about 5 to 10 minutes). Open http://localhost:4040 while it runs for the Spark UI.
4. **Task 3B, Airflow.** In the Airflow UI, unpause `olist\_delivery\_etl` and press the play button (*Trigger DAG*). Check the loaded table from a JupyterLab terminal or:

```bash
   docker exec -it capstone-postgres psql -U capstone -d olist -c "SELECT \* FROM mart\_delivery\_state\_month ORDER BY orders DESC LIMIT 5;"
   ```

5. **Task 3C, Kafka.** Open two JupyterLab terminals (*File > New > Terminal*):

```bash
   cd work/task3\_pipeline/kafka \&\& python producer.py     # terminal 2: creates the topic, sends 2,000 Black Friday 2017 events
   cd work/task3\_pipeline/kafka \&\& python consumer.py     # terminal 1, start while the producer is sending
   # python consumer.py --from-beginning   replays the whole topic with a fresh consumer group
   ```

   Then in Kafdrop click `olist-order-events`, *View Messages*, and the `delivery-monitor` consumer group.

6. `docker compose down` stops everything (`down -v` also deletes the database volume).

## Expected outputs

|Task|Output|Where|
|-|-|-|
|1A|Four result tables, two charts, before/after plans and a comparison table|saved in the notebook|
|1B|385-word design brief|`task1\_sql\_nosql/nosql\_design\_brief.md`|
|2|Cleaned 4.6M-row DataFrame, 4 KPIs, pruning benchmark, Spark UI screenshots|notebook, `evidence/spark\_ui\_\*.png`, `data/processed/`|
|3A|All containers running|`evidence/docker\_compose\_ps.png`|
|3B|Green four-task DAG run; table `mart\_delivery\_state\_month` (556 rows)|`evidence/airflow\_dag\_success.png`|
|3C|Producer and consumer terminal output; Kafdrop topic, messages and consumer lag|`task3\_pipeline/kafka/output/`, `evidence/kafdrop\_\*.png`|

## Task 3 notes

**Containerisation.** Every service is pinned to an image version in `docker-compose.yml`, so anyone with Docker gets the same Postgres 16, Airflow 2.9.3 and Kafka 7.4.0 regardless of their operating system. Containers find each other by service name (`postgres`, `kafka`) on the Compose network, configuration comes from `.env`, and named volumes keep the database between restarts. That is what makes the project portable (it runs the same on Windows, macOS and Linux) and reproducible (`docker compose up` rebuilds the same environment every time).

**Airflow task sequence.** `extract\_orders` reads the four CSVs needed and stages them for the run; `validate\_extract` stops the run if columns are missing, keys are null or duplicated, or a file is too small; `transform\_delivery\_mart` turns 96k delivered orders into 556 state-month rows with revenue, freight share, delivery days, late rate and review scores; `load\_to\_postgres` upserts them with `ON CONFLICT`, so re-running the DAG never duplicates data. Tasks pass file paths through XCom, not data.

**Kafka design.** The producer creates the topic with three partitions and keys every message by `order\_id`, so an order's events stay in order on one partition. It uses `acks="all"` so a send only counts once the broker has stored it. The consumer processes events in three ways (counting by type and state, filtering late deliveries into a log, flagging orders over R$1,000) and commits offsets in the group `delivery-monitor`, so stopping it and starting it again resumes where it left off.

**A Kafka detail worth knowing.** The lab compose file advertised only `localhost:9092`. That works for scripts on the host, but Kafdrop runs inside Docker, where `localhost` is Kafdrop itself. This stack uses two listeners, `kafka:29092` for containers and `localhost:9092` for the host.

## Security and GDPR

No passwords, keys or `.env` files are committed (see `.gitignore`). Analytical outputs contain no customer identifiers. See `docs/sustainability\_gdpr\_reflection.md`.

## AI-use declaration

AI assistance was used while developing parts of this portfolio. All code was run and verified, and the analysis and design choices were reviewed and are understood by the author, in line with the institutional AI-use policy.

