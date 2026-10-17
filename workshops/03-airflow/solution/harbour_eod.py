"""Harbour's end-of-day reconciliation: reference solution.

Five changes from the production version, each tied to a control:
  1. The business date comes from the run (`ds`), never from the clock. A backfill of
     13 to 16 Oct then processes 13, 14, 15 and 16 Oct, one each.
  2. Each write replaces that date's rows inside one transaction (delete, then insert),
     so a rerun produces the same result instead of adding to it.
  3. A sensor waits for NPCI's control file, which is written after the CSV is complete,
     instead of reading a CSV that may still be arriving.
  4. A gate compares what was loaded with the control file's totals and fails the run,
     without retrying, if they disagree. Nothing is published from a file that does not
     add up.
  5. Transient failures (a dropped connection) are retried; data failures are not.
"""
import csv
from datetime import timedelta
from decimal import Decimal

import pendulum
import psycopg
from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.sdk.exceptions import AirflowFailException

from harbour import dsn, eod

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
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=10)},
    tags=["harbour", "workshop-3"],
)
def harbour_eod():
    @task.sensor(poke_interval=5, timeout=eod.file_wait_seconds(), mode="poke", retries=0)
    def wait_for_npci_file(ds=None):
        # NPCI writes the control file last, so its presence means the CSV is complete.
        return PokeReturnValue(is_done=eod.control_path(ds).exists())

    @task
    def load_npci_raw(ds=None):
        with eod.csv_path(ds).open() as f:
            rows = [r for r in csv.DictReader(f) if r["txn_date"] == ds]
        with psycopg.connect(dsn()) as conn, conn.cursor() as cur:  # one transaction
            cur.execute("DELETE FROM stg_npci_raw WHERE txn_date = %s", (ds,))
            cur.executemany(
                "INSERT INTO stg_npci_raw (txn_date, rrn, account_id, dr_cr, amount, settled_at, response_code) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                [(r["txn_date"], r["rrn"], r["account_id"], r["dr_cr"], r["amount"], r["settled_at"], r["response_code"]) for r in rows],
            )
        print(f"loaded {len(rows)} records for {ds}")

    @task(retries=0)
    def check_control_totals(ds=None):
        ctl = eod.read_control(ds)
        with psycopg.connect(dsn()) as conn:
            n, cr, dr = conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(amount) FILTER (WHERE dr_cr = 'CR'), 0), COALESCE(SUM(amount) FILTER (WHERE dr_cr = 'DR'), 0) "
                "FROM stg_npci_raw WHERE txn_date = %s", (ds,)).fetchone()
        problems = []
        if n != ctl["records"]:
            problems.append(f"{n} records loaded, control file says {ctl['records']}")
        if Decimal(cr) != ctl["credit_total"]:
            problems.append(f"credits {cr} vs control {ctl['credit_total']}")
        if Decimal(dr) != ctl["debit_total"]:
            problems.append(f"debits {dr} vs control {ctl['debit_total']}")
        if problems:
            raise AirflowFailException(f"NPCI file for {ds} does not match its control file: " + "; ".join(problems))
        print(f"{n} records and both totals agree with the control file")

    @task
    def reconcile(ds=None):
        with psycopg.connect(dsn()) as conn:
            conn.execute("DELETE FROM recon_breaks WHERE business_date = %s", (ds,))
            conn.execute(RECON_SQL, {"d": ds})

    @task
    def publish_position(ds=None):
        with psycopg.connect(dsn()) as conn:
            conn.execute("DELETE FROM treasury_position WHERE business_date = %s", (ds,))
            conn.execute(POSITION_SQL, {"d": ds})
        print(f"published position for {ds}")

    wait_for_npci_file() >> load_npci_raw() >> check_control_totals() >> reconcile() >> publish_position()


harbour_eod()
