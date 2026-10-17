"""Harbour's UPI posting consumer: first draft.

Reads settled UPI transactions from Kafka and posts both legs to the core ledger.
It works on a quiet afternoon. Run `harbour check ws2` to see what happens on a real one.
"""
import json
import signal

import psycopg
from confluent_kafka import Consumer

import harbour

INSERT = """
INSERT INTO ledger_postings (txn_ref, leg, account_id, value_date, dr_cr, amount, channel, narration)
VALUES (%s, %s, %s, %s, %s, %s, 'UPI', %s)
"""

running = True
signal.signal(signal.SIGTERM, lambda *_: globals().update(running=False))


def post(cur, e):
    gl_side = "DR" if e["dr_cr"] == "CR" else "CR"
    narration = "UPI inward credit" if e["dr_cr"] == "CR" else "UPI outward debit"
    cur.execute(INSERT, (e["rrn"], "CUST", e["account_id"], e["txn_date"], e["dr_cr"], e["amount"], narration))
    cur.execute(INSERT, (e["rrn"], "GL", harbour.GL_UPI, e["txn_date"], gl_side, e["amount"], narration))


def main():
    consumer = Consumer(harbour.consumer_config(**{"enable.auto.commit": False}))
    consumer.subscribe([harbour.TOPIC])
    conn = psycopg.connect(harbour.dsn())
    print("posting consumer started", flush=True)
    try:
        while running:
            batch = consumer.consume(num_messages=200, timeout=1.0)
            if not batch:
                continue
            consumer.commit(asynchronous=False)  # acknowledge the batch
            with conn.cursor() as cur:
                for msg in batch:
                    if msg.error():
                        continue
                    post(cur, json.loads(msg.value()))
            conn.commit()
            print(f"posted {len(batch)} events", flush=True)
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
