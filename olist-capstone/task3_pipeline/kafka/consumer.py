"""
Delivery monitor: consumes olist-order-events and does three kinds of simple processing.

  1. Counting   running totals per event type and per customer state
  2. Filtering  delivered orders that missed their promised date go to late_deliveries.log
  3. Flagging   high-value orders (>= R$1,000) are printed as they are purchased

Every 250 messages it prints a live summary; on exit it writes output/consumer_summary.json.

    python consumer.py                    # runs until Ctrl+C or 15 s with no new messages
    python consumer.py --from-beginning   # new consumer group, replays the whole topic
"""
import argparse
import json
import os
import logging
import time
import uuid
from collections import Counter
from pathlib import Path

from kafka import KafkaConsumer

TOPIC = "olist-order-events"
HIGH_VALUE_BRL = 1000.0
OUT_DIR = Path(__file__).resolve().parent / "output"


class DeliveryMonitor:
    """All processing logic, kept separate from Kafka so it can be unit-tested."""

    def __init__(self):
        self.by_type = Counter()
        self.by_state = Counter()
        self.delivered = 0
        self.late = []
        self.high_value = []
        self.revenue_purchased = 0.0

    def process(self, event: dict) -> list[str]:
        alerts = []
        etype = event["event_type"]
        self.by_type[etype] += 1
        if etype == "order_purchased":
            self.by_state[event["customer_state"]] += 1
            value = event.get("order_value_brl") or 0
            self.revenue_purchased += value
            if value >= HIGH_VALUE_BRL:
                self.high_value.append(event["order_id"])
                alerts.append(f"HIGH VALUE  R${value:,.2f} order {event['order_id'][:8]} from {event['customer_state']}")
        elif etype == "order_delivered":
            self.delivered += 1
            if event.get("days_late", 0) > 0:
                self.late.append(event)
                alerts.append(f"LATE        {event['days_late']} day(s) late, order {event['order_id'][:8]} "
                              f"to {event['customer_state']} (promised {event['promised_date']})")
        return alerts

    def summary(self) -> dict:
        late_rate = 100 * len(self.late) / self.delivered if self.delivered else 0.0
        return {
            "events_by_type": dict(self.by_type),
            "total_events": sum(self.by_type.values()),
            "purchases_by_state_top5": dict(self.by_state.most_common(5)),
            "purchased_revenue_brl": round(self.revenue_purchased, 2),
            "delivered_orders": self.delivered,
            "late_deliveries": len(self.late),
            "late_rate_pct": round(late_rate, 2),
            "high_value_orders": len(self.high_value),
        }


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--group", default="delivery-monitor")
    p.add_argument("--from-beginning", action="store_true", help="use a fresh group id to replay everything")
    p.add_argument("--idle-timeout", type=float, default=15, help="stop after this many seconds without messages")
    return p.parse_args()


def main():
    args = parse_args()
    OUT_DIR.mkdir(exist_ok=True)
    logging.basicConfig(filename=OUT_DIR / "late_deliveries.log", level=logging.WARNING,
                        format="%(asctime)s %(message)s")
    group = f"{args.group}-{uuid.uuid4().hex[:6]}" if args.from_beginning else args.group

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=args.bootstrap,
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=int(args.idle_timeout * 1000),
    )
    monitor = DeliveryMonitor()
    print(f"consumer group '{group}' listening on {TOPIC} (stops after {args.idle_timeout:.0f}s idle)")
    started = time.time()
    try:
        for message in consumer:
            for alert in monitor.process(message.value):
                print(f"  p{message.partition} offset {message.offset:>5} | {alert}")
                if alert.startswith("LATE"):
                    logging.warning(alert)
            n = sum(monitor.by_type.values())
            if n % 250 == 0:
                s = monitor.summary()
                print(f"--- {n} events | delivered {s['delivered_orders']} | late {s['late_deliveries']} "
                      f"({s['late_rate_pct']}%) | high value {s['high_value_orders']} | by type {s['events_by_type']}")
    except KeyboardInterrupt:
        print("stopped by user")
    finally:
        consumer.close()
        summary = monitor.summary()
        summary["seconds_running"] = round(time.time() - started, 1)
        summary["consumer_group"] = group
        (OUT_DIR / "consumer_summary.json").write_text(json.dumps(summary, indent=2))
        print("\nFinal summary")
        print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
