"""harbour: run Harbour Bank's workshop stack.

  harbour doctor          check Kafka and Postgres are reachable
  harbour reset           ledger back to 06:00, topics recreated, consumer group removed
  harbour publish         publish 16 Oct's UPI events to Kafka
  harbour status          ledger postings, consumer lag and dead-letter count
  harbour check ws2 [--consumer PATH] [--only crash|scale]
"""
import argparse
import json
import sys

from . import DLQ, GROUP, TOPIC, bootstrap, db, dsn, kafka_ops
from .data import fmt_inr, load


def cmd_doctor(_):
    ok = True
    try:
        kafka_ops.wait_ready(20)
        print(f"Kafka     ok   {bootstrap()}")
    except Exception as e:
        ok = False
        print(f"Kafka     FAIL {e}")
    try:
        with db.connect() as c:
            v = c.execute("SHOW server_version").fetchone()[0]
        print(f"Postgres  ok   {v} at {dsn().rsplit('@', 1)[-1]}")
    except Exception as e:
        ok = False
        print(f"Postgres  FAIL {e}")
    return 0 if ok else 1


def cmd_reset(_):
    db.reset()
    kafka_ops.reset_topics()
    print(f"Ledger reset to 06:00. Topics {TOPIC} and {DLQ} recreated. Consumer group {GROUP} removed.")
    return 0


def cmd_publish(_):
    n = kafka_ops.publish()
    print(f"Published {n:,} messages to {TOPIC}: 1,800 settled transactions, the switch's redeliveries, and whatever else arrived.")
    return 0


def cmd_status(_):
    with db.connect() as c:
        rows = db.upi_rows(c)
        gl = db.gl_net(c)
    net = load()["npci_net_settlement"]["net_amount"]
    print(f"UPI postings for 16 Oct   {rows:,} legs ({rows // 2:,} transactions) of 3,600")
    print(f"Settlement GL             {fmt_inr(-gl)} payable   NPCI {fmt_inr(-net)} payable")
    try:
        lg = kafka_ops.lag()
        print("Consumer lag              " + "  ".join(f"p{p}:{hi - cm}" for p, (cm, hi) in sorted(lg.items())) + f"   total {sum(hi - cm for cm, hi in lg.values()):,}")
    except Exception as e:
        print(f"Consumer lag              unavailable ({e})")
    print(f"Dead-letter topic         {len(kafka_ops.read_dlq(3))} message(s)")
    return 0


def cmd_check(a):
    if a.workshop != "ws2":
        print("Only ws2 has checks so far.")
        return 2
    from .checks import ws2
    scenarios = (a.only,) if a.only else ("crash", "scale")
    results = ws2.run(a.consumer, scenarios, verbose=not a.json)
    if a.json:
        print(json.dumps([{"scenario": r.scenario, "ok": r.ok, "controls": [c.__dict__ for c in r.controls], "notes": r.notes} for r in results]))
    else:
        ok = all(r.ok for r in results)
        print("\n" + ("All controls pass. Risk can sign off the consumer." if ok else "Not ready for go-live. Fix the failing controls and run the check again."))
    return 0 if all(r.ok for r in results) else 1


def main(argv=None):
    p = argparse.ArgumentParser(prog="harbour", description="Harbour Bank workshop stack")
    s = p.add_subparsers(dest="cmd", required=True)
    s.add_parser("doctor").set_defaults(f=cmd_doctor)
    s.add_parser("reset").set_defaults(f=cmd_reset)
    s.add_parser("publish").set_defaults(f=cmd_publish)
    s.add_parser("status").set_defaults(f=cmd_status)
    c = s.add_parser("check")
    c.add_argument("workshop", choices=["ws2"])
    c.add_argument("--consumer", default="workshops/02-kafka/consumer.py")
    c.add_argument("--only", choices=["crash", "scale"])
    c.add_argument("--json", action="store_true")
    c.set_defaults(f=cmd_check)
    a = p.parse_args(argv)
    try:
        return a.f(a)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
