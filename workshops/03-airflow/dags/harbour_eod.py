"""Harbour's end-of-day reconciliation: the version in production since Tuesday's deploy.

Each night: load NPCI's raw file for the business date, reconcile it to the ledger, and
publish the settlement position Treasury reads at 09:00. Run `harbour check ws3` to see
what this does to a backfill, a rerun and a late file.
"""
import csv
from datetime import date, timedelta

import pendulum
import psycopg
from airflow.sdk import dag, task

from harbour import dsn, eod

BUSINESS_DATE = (date.today() - timedelta(days=1)).isoformat()  # last night's business date

RECON_SQL = """
INSERT INTO recon_breaks (business_date, rrn, account_id, dr_cr, amount, break_type)
SELECT %(d)s, n.rrn, n.account_id, n.dr_cr, n.amount, 'NOT_IN_LEDGER'
  FROM stg_npci_raw n
 WHERE n.txn_date = %(d)s
   AND NOT EXISTS (SELECT 1 FROM ledger_postings l WHERE l.txn_ref = n.rrn AND l.leg = 'CUST' AND l.channel = 'UPI')
UNION ALL
SELECT %(d)s, l.txn_ref, l.account_id, l.dr_cr, l.amount, 'DUPLICATE_IN_LEDGER'
  FROM ledger_postings l
 WHERE l.channel = 'UPI' AND l.leg = 'CUST' AND l.value_date = %(d)s
 GROUP BY l.txn_ref, l.account_id, l.dr_cr, l.amount
HAVING COUNT(*) > 1
UNION ALL
SELECT %(d)s, l.txn_ref, l.account_id, l.dr_cr, l.amount, 'NOT_AT_NPCI'
  FROM ledger_postings l
 WHERE l.channel = 'UPI' AND l.leg = 'CUST' AND l.value_date = %(d)s
   AND NOT EXISTS (SELECT 1 FROM stg_npci_raw n WHERE n.rrn = l.txn_ref AND n.txn_date = %(d)s)
"""

POSITION_SQL = """
INSERT INTO treasury_position (business_date, npci_net, gl_net, difference, breaks)
WITH npci AS (SELECT COALESCE(SUM(CASE WHEN dr_cr = 'CR' THEN amount ELSE -amount END), 0) AS net
                FROM stg_npci_raw WHERE txn_date = %(d)s),
     gl   AS (SELECT COALESCE(SUM(CASE WHEN dr_cr = 'DR' THEN amount ELSE -amount END), 0) AS net
                FROM ledger_postings WHERE channel = 'UPI' AND leg = 'GL' AND value_date = %(d)s)
SELECT %(d)s, npci.net, gl.net, npci.net - gl.net,
       (SELECT COUNT(*) FROM recon_breaks WHERE business_date = %(d)s)
  FROM npci, gl
"""


@dag(
    dag_id="harbour_eod",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 13, tz="UTC"),
    end_date=pendulum.datetime(2026, 10, 16, tz="UTC"),  # the workshop covers 13 to 16 Oct only
    catchup=False,
    tags=["harbour", "workshop-3"],
)
def harbour_eod():
    @task
    def load_npci_raw():
        with psycopg.connect(dsn()) as conn, conn.cursor() as cur, eod.csv_path(BUSINESS_DATE).open() as f:
            rows = list(csv.DictReader(f))
            cur.executemany(
                "INSERT INTO stg_npci_raw (txn_date, rrn, account_id, dr_cr, amount, settled_at, response_code) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                [(r["txn_date"], r["rrn"], r["account_id"], r["dr_cr"], r["amount"], r["settled_at"], r["response_code"]) for r in rows],
            )
        print(f"loaded {len(rows)} records for {BUSINESS_DATE}")

    @task
    def reconcile():
        with psycopg.connect(dsn()) as conn:
            conn.execute(RECON_SQL, {"d": BUSINESS_DATE})

    @task
    def publish_position():
        with psycopg.connect(dsn()) as conn:
            conn.execute(POSITION_SQL, {"d": BUSINESS_DATE})
        print(f"published position for {BUSINESS_DATE}")

    load_npci_raw() >> reconcile() >> publish_position()


harbour_eod()
