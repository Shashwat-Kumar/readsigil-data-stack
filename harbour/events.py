"""The UPI switch's event stream for 16 Oct, as Harbour's Kafka topic receives it.

Real streams are at-least-once: the switch republishes some events after a timeout,
so the same RRN arrives more than once. One message was cut off mid-write and is not
valid JSON. Events are keyed by account, so one account's events stay in order on one
partition."""
import json
import random

from .data import load

REDELIVERIES = 37
POISON_AT = 911  # position in the stream
POISON_KEY = "HB100150"
POISON = b'{"event_id":"evt-628916402217","rrn":"628916402217","account_id":"HB100150","dr_cr":"CR","amou'


def event(r: dict) -> dict:
    return {
        "event_id": f"evt-{r['rrn']}",
        "rrn": r["rrn"],
        "account_id": r["account_id"],
        "dr_cr": r["dr_cr"],
        "amount": f"{r['amount'] / 100:.2f}",
        "txn_date": r["txn_date"],
        "settled_at": r["settled_at"],
        "response_code": r["response_code"],
    }


def stream() -> list:
    """[(key, value_bytes)] in publish order."""
    raw = load()["npci_raw"]
    out = [(r["account_id"], json.dumps(event(r), separators=(",", ":")).encode()) for r in raw]
    rng = random.Random(20261016)
    picks = sorted(rng.sample(range(len(raw) - 120), REDELIVERIES))
    inserts = []
    for i in picks:
        inserts.append((i + rng.randint(15, 110), out[i]))  # same bytes, a little later
    inserts.append((POISON_AT, (POISON_KEY, POISON)))
    for pos, item in sorted(inserts, key=lambda x: x[0], reverse=True):
        out.insert(pos, item)
    return out


def valid_rrns() -> set:
    return {r["rrn"] for r in load()["npci_raw"]}
