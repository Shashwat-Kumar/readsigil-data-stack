"""harbour: run Harbour Bank's workshop stack.

  harbour doctor          check Kafka and Postgres are reachable
  harbour reset           ledger back to 06:00, topics recreated, consumer group removed
  harbour publish         publish 16 Oct's UPI events to Kafka
  harbour status          ledger postings, consumer lag and dead-letter count
  harbour check ws2 [--consumer PATH] [--only crash|scale]

  harbour ws3 reset       ledger as of Saturday, landing folder with 13 to 16 Oct files
  harbour ws3 arrive DATE NPCI finishes writing DATE's file (use for 16 Oct)
  harbour ws3 correct     NPCI's corrected 14 Oct file arrives
  harbour ws3 status      staging, breaks and published positions by date
  harbour check ws3 [--dag PATH]
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


def cmd_ws3(a):
    from . import eod
    if a.action == "reset":
        eod.reset()
        print(f"Ledger set to Saturday morning. Landing folder {eod.landing()}:")
        for p in sorted(eod.landing().glob("upi_raw_*")):
            print(f"  {p.name}")
        print("16 Oct's CSV is still being written; run `harbour ws3 arrive 2026-10-16` to finish it.")
    elif a.action == "arrive":
        eod.land(a.date or eod.LATE_DATE, "full")
        print(f"NPCI finished writing {a.date or eod.LATE_DATE}: CSV complete, control file written.")
    elif a.action == "correct":
        eod.land(eod.SHORT_DATE, "full")
        print("NPCI's corrected 14 Oct file has arrived.")
    elif a.action == "status":
        with db.connect() as c:
            rows = c.execute("""
                SELECT d::date, (SELECT COUNT(*) FROM stg_npci_raw WHERE txn_date = d::date),
                       (SELECT COUNT(*) FROM recon_breaks WHERE business_date = d::date),
                       (SELECT COUNT(*) FROM treasury_position WHERE business_date = d::date),
                       (SELECT difference FROM treasury_position WHERE business_date = d::date ORDER BY published_at DESC LIMIT 1)
                  FROM generate_series(DATE '2026-10-13', DATE '2026-10-16', INTERVAL '1 day') d""").fetchall()
        print("date        staged  breaks  positions  latest difference")
        for d, n, b, p, diff in rows:
            print(f"{d}  {n:>6,}  {b:>6}  {p:>9}  {'' if diff is None else fmt_inr(int(diff * 100))}")
    return 0


def cmd_check(a):
    if a.workshop == "ws3":
        from .checks import ws3
        r = ws3.run(a.dag or "workshops/03-airflow/dags/harbour_eod.py", verbose=not a.json)
        if a.json:
            print(json.dumps({"ok": r.ok, "controls": [c.__dict__ for c in r.controls], "runs": [list(x) for x in r.runs]}))
        else:
            print("\n" + ("All controls pass. Treasury can rely on the position table." if r.ok else "Not ready. Fix the failing controls and run the check again."))
        return 0 if r.ok else 1
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
    c.add_argument("workshop", choices=["ws2", "ws3"])
    c.add_argument("--consumer", default="workshops/02-kafka/consumer.py")
    c.add_argument("--dag")
    c.add_argument("--only", choices=["crash", "scale"])
    c.add_argument("--json", action="store_true")
    c.set_defaults(f=cmd_check)
    w3 = s.add_parser("ws3")
    w3.add_argument("action", choices=["reset", "arrive", "correct", "status"])
    w3.add_argument("date", nargs="?")
    w3.set_defaults(f=cmd_ws3)
    a = p.parse_args(argv)
    try:
        return a.f(a)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
