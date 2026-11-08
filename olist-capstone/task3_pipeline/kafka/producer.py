"""
Replay Olist orders as a live stream of order-lifecycle events.

Each order becomes up to four events (purchased, approved, shipped, delivered, or
canceled), and all events are sent in true chronological order, so the topic looks
like what a real marketplace would emit. By default it replays Black Friday week 2017,
the same spike found in SQL Query 2.

Messages are keyed by order_id, so every event for one order lands on the same
partition and is read back in order.

    python producer.py                       # 2,000 events at 25 per second
    python producer.py --limit 500 --rate 50
"""
import argparse
import json
import os
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

TOPIC = "olist-order-events"
DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "olist"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--start", default="2017-11-20", help="replay events from this date")
    p.add_argument("--limit", type=int, default=2000, help="number of events to send")
    p.add_argument("--rate", type=float, default=25, help="events per second (0 = as fast as possible)")
    p.add_argument("--partitions", type=int, default=3)
    return p.parse_args()


def ensure_topic(bootstrap, partitions):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="olist-admin")
    try:
        admin.create_topics([NewTopic(TOPIC, num_partitions=partitions, replication_factor=1)])
        print(f"created topic {TOPIC} with {partitions} partitions")
    except TopicAlreadyExistsError:
        print(f"topic {TOPIC} already exists")
    finally:
        admin.close()


def build_events(start: str) -> pd.DataFrame:
    """Turn order rows into one row per lifecycle event, sorted by event time."""
    orders = pd.read_csv(DATA_DIR / "olist_orders_dataset.csv", parse_dates=[
        "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date"])
    customers = pd.read_csv(DATA_DIR / "olist_customers_dataset.csv", usecols=["customer_id", "customer_state"])
    items = (pd.read_csv(DATA_DIR / "olist_order_items_dataset.csv")
             .groupby("order_id").agg(order_value=("price", "sum"), items=("order_item_id", "count"))
             .reset_index())
    # customer_id is used for the join and then dropped: events carry no personal identifiers
    orders = orders.merge(customers, on="customer_id").merge(items, on="order_id", how="left").drop(columns="customer_id")

    stages = [("order_purchased", "order_purchase_timestamp"),
              ("order_approved", "order_approved_at"),
              ("order_shipped", "order_delivered_carrier_date"),
              ("order_delivered", "order_delivered_customer_date")]
    frames = []
    for event_type, ts_col in stages:
        f = orders[orders[ts_col].notna()].copy()
        f["event_type"], f["event_time"] = event_type, f[ts_col]
        frames.append(f)
    canceled = orders[orders.order_status == "canceled"].copy()
    canceled["event_type"], canceled["event_time"] = "order_canceled", canceled["order_purchase_timestamp"]
    frames.append(canceled)

    events = pd.concat(frames, ignore_index=True)
    events = events[events.event_time >= start].sort_values("event_time")
    return events


def to_message(row) -> dict:
    msg = {
        "event_type": row.event_type,
        "event_time": row.event_time.isoformat(),
        "order_id": row.order_id,
        "customer_state": row.customer_state,
        "order_value_brl": round(float(row.order_value), 2) if pd.notna(row.order_value) else None,
        "items": int(row.items) if pd.notna(row.items) else 0,
    }
    if row.event_type == "order_delivered":
        promised = row.order_estimated_delivery_date
        days_late = (row.order_delivered_customer_date.normalize() - promised).days
        msg["promised_date"] = promised.date().isoformat()
        msg["days_late"] = int(days_late)
    return msg


def main():
    args = parse_args()
    ensure_topic(args.bootstrap, args.partitions)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",            # wait for the broker to confirm every write
        linger_ms=20,          # small batching window for throughput
    )
    events = build_events(args.start).head(args.limit)
    print(f"replaying {len(events):,} events from {args.start} to topic {TOPIC} at {args.rate or 'max'} events/s")

    delay = 1 / args.rate if args.rate else 0
    sent = 0
    try:
        for row in events.itertuples(index=False):
            msg = to_message(row)
            meta = producer.send(TOPIC, key=msg["order_id"], value=msg).get(timeout=10)
            sent += 1
            if sent <= 5 or sent % 250 == 0:
                print(f"[{sent:>5}] p{meta.partition} offset {meta.offset:>5}  {msg['event_time']}  "
                      f"{msg['event_type']:<16} {msg['customer_state']}  R${msg['order_value_brl']}")
            if delay:
                time.sleep(delay)
    except KeyboardInterrupt:
        print("stopped by user")
    finally:
        producer.flush()
        producer.close()
        print(f"done: {sent:,} events sent")


if __name__ == "__main__":
    main()
