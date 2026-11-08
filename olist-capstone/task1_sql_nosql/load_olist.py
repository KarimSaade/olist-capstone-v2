"""Load the seven Olist CSV files into PostgreSQL.

Usage (from the repo root):
    python task1_sql_nosql/load_olist.py

Connection details come from the OLIST_PG_URL environment variable, e.g.
    postgresql://lab_user:lab_pass@localhost:5432/olist
so no credentials live in the code.
"""
import os
from pathlib import Path

import psycopg2

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data" / "olist"
SCHEMA_FILE = Path(__file__).with_name("schema.sql")

# Load order matters because of the foreign keys.
TABLE_FILES = [
    ("customers", "olist_customers_dataset.csv"),
    ("sellers", "olist_sellers_dataset.csv"),
    ("category_translation", "product_category_name_translation.csv"),
    ("products", "olist_products_dataset.csv"),
    ("orders", "olist_orders_dataset.csv"),
    ("order_items", "olist_order_items_dataset.csv"),
    ("order_reviews", "olist_order_reviews_dataset.csv"),
]

# Two categories appear in products but are missing from the translation file.
MISSING_TRANSLATIONS = {
    "pc_gamer": "pc_gamer",
    "portateis_cozinha_e_preparadores_de_alimentos": "portable_kitchen_and_food_preparers",
}


def get_connection():
    url = os.environ.get("OLIST_PG_URL")
    if not url:
        raise SystemExit("Set OLIST_PG_URL, e.g. postgresql://user:password@localhost:5432/olist")
    return psycopg2.connect(url)


def load_all(verbose=True):
    """Recreate the schema and bulk load every CSV with COPY. Returns row counts."""
    counts = {}
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_FILE.read_text())
        for table, file_name in TABLE_FILES:
            with open(DATA_DIR / file_name, encoding="utf-8-sig") as fh:
                cur.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT csv, HEADER true)", fh)
            if table == "category_translation":
                cur.executemany(
                    "INSERT INTO category_translation VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    list(MISSING_TRANSLATIONS.items()),
                )
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cur.fetchone()[0]
        cur.execute("ANALYZE")  # fresh planner statistics after a bulk load
    if verbose:
        for table, n in counts.items():
            print(f"{table:<22}{n:>10,} rows")
    return counts


if __name__ == "__main__":
    load_all()
