"""
Olist delivery-performance ETL
==============================

extract_orders  >>  validate_extract  >>  transform_delivery_mart  >>  load_to_postgres

Extract   Read the four Olist CSVs the mart needs (orders, items, customers, reviews)
          and stage a copy for this run.
Validate  Fail fast if columns are missing, keys are null or duplicated, or the file
          is suspiciously small, so bad data never reaches the warehouse.
Transform Build one row per (customer state, purchase month) with orders, revenue,
          freight share, delivery days, late-delivery rate and review scores.
          Customer identifiers are dropped here: the output is aggregated only.
Load      Upsert into olist.mart_delivery_state_month (idempotent, so a re-run
          replaces the same rows instead of duplicating them).

Files are passed between tasks by path through XCom; only small strings go into XCom.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_DIR = Path(os.environ.get("OLIST_DATA_DIR", "/opt/airflow/data/olist"))
STAGING_ROOT = Path(os.environ.get("OLIST_STAGING_DIR", "/opt/airflow/data/staging"))
POSTGRES_CONN_ID = "olist_dw"
TARGET_TABLE = "mart_delivery_state_month"

SOURCES = {
    "orders": ("olist_orders_dataset.csv",
               ["order_id", "customer_id", "order_status", "order_purchase_timestamp",
                "order_delivered_customer_date", "order_estimated_delivery_date"]),
    "items": ("olist_order_items_dataset.csv", ["order_id", "price", "freight_value"]),
    "customers": ("olist_customers_dataset.csv", ["customer_id", "customer_state"]),
    "reviews": ("olist_order_reviews_dataset.csv", ["order_id", "review_score"]),
}
MIN_ROWS = {"orders": 50_000, "items": 50_000, "customers": 50_000, "reviews": 50_000}

default_args = {
    "owner": "capstone",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _staging_dir(context) -> Path:
    path = STAGING_ROOT / context["run_id"].replace(":", "_").replace("+", "_")
    path.mkdir(parents=True, exist_ok=True)
    return path


# ---------------------------------------------------------------- Extract
def extract(**context) -> dict:
    import pandas as pd

    staging = _staging_dir(context)
    paths = {}
    for name, (file_name, columns) in SOURCES.items():
        df = pd.read_csv(DATA_DIR / file_name, usecols=columns)
        out = staging / f"{name}.csv"
        df.to_csv(out, index=False)
        paths[name] = str(out)
        print(f"extracted {name:<10} {len(df):>8,} rows -> {out}")
    return paths  # pushed to XCom


# ---------------------------------------------------------------- Validate
def validate(**context) -> dict:
    import pandas as pd

    paths = context["ti"].xcom_pull(task_ids="extract_orders")
    problems = []
    for name, path in paths.items():
        expected = SOURCES[name][1]
        df = pd.read_csv(path)
        missing = set(expected) - set(df.columns)
        if missing:
            problems.append(f"{name}: missing columns {sorted(missing)}")
            continue
        if len(df) < MIN_ROWS[name]:
            problems.append(f"{name}: only {len(df):,} rows (expected >= {MIN_ROWS[name]:,})")
        key = "customer_id" if name == "customers" else "order_id"
        if df[key].isna().any():
            problems.append(f"{name}: {df[key].isna().sum()} null {key} values")
        if name in ("orders", "customers") and df[key].duplicated().any():
            problems.append(f"{name}: duplicate {key} values")
        print(f"validated {name:<10} {len(df):>8,} rows")
    if problems:
        raise ValueError("Validation failed:\n  " + "\n  ".join(problems))
    return paths


# ---------------------------------------------------------------- Transform
def transform(**context) -> str:
    import pandas as pd

    paths = context["ti"].xcom_pull(task_ids="validate_extract")
    ts_cols = ["order_purchase_timestamp", "order_delivered_customer_date", "order_estimated_delivery_date"]
    orders = pd.read_csv(paths["orders"], parse_dates=ts_cols)
    items = pd.read_csv(paths["items"])
    customers = pd.read_csv(paths["customers"])
    reviews = pd.read_csv(paths["reviews"])

    # Only delivered orders have a real delivery date
    orders = orders[(orders.order_status == "delivered") & orders.order_delivered_customer_date.notna()].copy()
    orders["delivery_days"] = (orders.order_delivered_customer_date
                               - orders.order_purchase_timestamp).dt.total_seconds() / 86_400
    orders["is_late"] = orders.order_delivered_customer_date.dt.normalize() > orders.order_estimated_delivery_date
    orders["purchase_month"] = orders.order_purchase_timestamp.dt.to_period("M").dt.to_timestamp()

    order_value = items.groupby("order_id", as_index=False).agg(revenue=("price", "sum"),
                                                                freight=("freight_value", "sum"))
    order_review = reviews.groupby("order_id", as_index=False).review_score.mean()

    enriched = (orders
                .merge(customers, on="customer_id", how="left")
                .merge(order_value, on="order_id", how="left")
                .merge(order_review, on="order_id", how="left")
                .drop(columns=["customer_id"]))            # data minimisation: no customer IDs downstream

    mart = (enriched.groupby(["customer_state", "purchase_month"], as_index=False)
            .agg(orders=("order_id", "nunique"),
                 revenue_brl=("revenue", "sum"),
                 freight_brl=("freight", "sum"),
                 avg_delivery_days=("delivery_days", "mean"),
                 late_orders=("is_late", "sum"),
                 avg_review_score=("review_score", "mean"),
                 low_reviews=("review_score", lambda s: (s <= 2).sum())))
    mart["late_rate_pct"] = 100 * mart.late_orders / mart.orders
    mart["freight_pct_of_revenue"] = 100 * mart.freight_brl / mart.revenue_brl
    mart = mart.round({"revenue_brl": 2, "freight_brl": 2, "avg_delivery_days": 2, "avg_review_score": 3,
                       "late_rate_pct": 2, "freight_pct_of_revenue": 2})
    mart["purchase_month"] = mart.purchase_month.dt.date
    mart["loaded_at"] = datetime.utcnow().replace(microsecond=0)

    out = _staging_dir(context) / "mart_delivery_state_month.csv"
    mart.to_csv(out, index=False)
    print(f"transformed {len(enriched):,} delivered orders into {len(mart):,} state-month rows")
    print(mart.sort_values("orders", ascending=False).head(5).to_string(index=False))
    return str(out)


# ---------------------------------------------------------------- Load
DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    customer_state          CHAR(2)        NOT NULL,
    purchase_month          DATE           NOT NULL,
    orders                  INTEGER        NOT NULL,
    revenue_brl             NUMERIC(14, 2),
    freight_brl             NUMERIC(14, 2),
    avg_delivery_days       NUMERIC(6, 2),
    late_orders             INTEGER,
    avg_review_score        NUMERIC(4, 3),
    low_reviews             INTEGER,
    late_rate_pct           NUMERIC(6, 2),
    freight_pct_of_revenue  NUMERIC(6, 2),
    loaded_at               TIMESTAMP      NOT NULL,
    PRIMARY KEY (customer_state, purchase_month)
);
"""
COLUMNS = ["customer_state", "purchase_month", "orders", "revenue_brl", "freight_brl", "avg_delivery_days",
           "late_orders", "avg_review_score", "low_reviews", "late_rate_pct", "freight_pct_of_revenue", "loaded_at"]


def load(**context) -> int:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    csv_path = context["ti"].xcom_pull(task_ids="transform_delivery_mart")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    col_list = ", ".join(COLUMNS)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in COLUMNS[2:])
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute(f"CREATE TEMP TABLE staging_mart (LIKE {TARGET_TABLE} INCLUDING DEFAULTS) ON COMMIT DROP")
        with open(csv_path) as fh:
            cur.copy_expert(f"COPY staging_mart ({col_list}) FROM STDIN WITH (FORMAT csv, HEADER true)", fh)
        cur.execute(f"""
            INSERT INTO {TARGET_TABLE} ({col_list})
            SELECT {col_list} FROM staging_mart
            ON CONFLICT (customer_state, purchase_month) DO UPDATE SET {updates}
        """)
        upserted = cur.rowcount
        cur.execute(f"SELECT COUNT(*), SUM(orders) FROM {TARGET_TABLE}")
        total_rows, total_orders = cur.fetchone()
    print(f"upserted {upserted} rows; {TARGET_TABLE} now holds {total_rows} rows covering {total_orders:,} orders")
    return upserted


with DAG(
    dag_id="olist_delivery_etl",
    description="Olist CSV -> validate -> state/month delivery mart -> PostgreSQL",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["capstone", "etl", "olist"],
    doc_md=__doc__,
) as dag:
    extract_task = PythonOperator(task_id="extract_orders", python_callable=extract)
    validate_task = PythonOperator(task_id="validate_extract", python_callable=validate)
    transform_task = PythonOperator(task_id="transform_delivery_mart", python_callable=transform)
    load_task = PythonOperator(task_id="load_to_postgres", python_callable=load)

    extract_task >> validate_task >> transform_task >> load_task
