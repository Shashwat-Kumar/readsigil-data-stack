"""Harbour's UPI posting consumer: reference solution.

Three changes from the first draft, each tied to a control:
  1. Post with ON CONFLICT DO NOTHING, so a redelivered event is a no-op.
  2. Commit the Kafka offset only after the ledger transaction has committed.
     A crash then replays the event at worst, and change 1 makes the replay harmless.
  3. Park messages that cannot be parsed in the dead-letter topic, with headers that
     say where they came from and why, then move on.
"""
import json
import signal
from decimal import Decimal, InvalidOperation

import psycopg
from confluent_kafka import Consumer, Producer

import harbour

INSERT = """
INSERT INTO ledger_postings (txn_ref, leg, account_id, value_date, dr_cr, amount, channel, narration)
VALUES (%s, %s, %s, %s, %s, %s, 'UPI', %s)
ON CONFLICT (txn_ref, leg) DO NOTHING
"""
REQUIRED = ("rrn", "account_id", "dr_cr", "amount", "txn_date")

running = True
signal.signal(signal.SIGTERM, lambda *_: globals().update(running=False))


def parse(raw: bytes) -> dict:
    try:
        e = json.loads(raw)
    except (ValueError, TypeError) as err:
        raise ValueError(f"not valid JSON: {err}") from None
    missing = [k for k in REQUIRED if not e.get(k)]
    if missing:
        raise ValueError(f"missing fields: {', '.join(missing)}")
    if e["dr_cr"] not in ("DR", "CR"):
        raise ValueError(f"dr_cr must be DR or CR, got {e['dr_cr']!r}")
    try:
        if Decimal(e["amount"]) <= 0:
            raise ValueError("amount must be positive")
    except InvalidOperation:
        raise ValueError(f"amount is not a number: {e['amount']!r}") from None
    return e


def post(cur, e):
    gl_side = "DR" if e["dr_cr"] == "CR" else "CR"
    narration = "UPI inward credit" if e["dr_cr"] == "CR" else "UPI outward debit"
    cur.execute(INSERT, (e["rrn"], "CUST", e["account_id"], e["txn_date"], e["dr_cr"], e["amount"], narration))
    cur.execute(INSERT, (e["rrn"], "GL", harbour.GL_UPI, e["txn_date"], gl_side, e["amount"], narration))


def park(producer, msg, reason: str):
    headers = [
        ("error", reason.encode()),
        ("source-topic", msg.topic().encode()),
        ("source-partition", str(msg.partition()).encode()),
        ("source-offset", str(msg.offset()).encode()),
    ]
    producer.produce(harbour.DLQ, key=msg.key(), value=msg.value(), headers=headers)
    if producer.flush(10):
        raise RuntimeError("dead-letter topic did not acknowledge; not committing the offset")


def main():
    consumer = Consumer(harbour.consumer_config(**{"enable.auto.commit": False}))
    producer = Producer(harbour.producer_config())
    consumer.subscribe([harbour.TOPIC])
    conn = psycopg.connect(harbour.dsn())
    print("posting consumer started", flush=True)
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"kafka error: {msg.error()}", flush=True)
                continue
            try:
                event = parse(msg.value())
            except ValueError as err:
                park(producer, msg, str(err))
                print(f"parked partition {msg.partition()} offset {msg.offset()}: {err}", flush=True)
            else:
                with conn.transaction(), conn.cursor() as cur:
                    post(cur, event)
            consumer.commit(message=msg, asynchronous=False)  # only after the ledger has it
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
