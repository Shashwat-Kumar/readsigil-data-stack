"""Workshop 3 checks: run Harbour's end-of-day DAG on real Airflow the way operations does,
then audit what Treasury would read.

  1. Backfill 13 to 16 Oct, one Airflow DAG run per business date, in order.
     14 Oct's file is one record short of its control totals.
     16 Oct's file is still being written when its run starts and completes 20 s later.
  2. Rerun 15 and 16 Oct, as on-call does after a wobble.
  3. NPCI delivers a corrected 14 Oct file; rerun 14 Oct.

Each run is `airflow dags test harbour_eod <date>`: a real DAG run with the real task
runner, sensors and retries, without needing a scheduler."""
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from decimal import Decimal

import psycopg

from .. import dsn
from .. import eod
from ..data import fmt_inr

DAG_ID = "harbour_eod"


@dataclass
class Control:
    name: str
    ok: bool
    detail: str


@dataclass
class Result:
    controls: list = field(default_factory=list)
    runs: list = field(default_factory=list)  # (label, date, exit_code, tail)

    @property
    def ok(self):
        return bool(self.controls) and all(c.ok for c in self.controls)


class Airflow:
    def __init__(self, dag_file: str, wait_seconds: int):
        self.home = tempfile.mkdtemp(prefix="harbour-airflow-")
        self.dags = os.path.join(self.home, "dags")
        os.makedirs(self.dags)
        shutil.copy(dag_file, os.path.join(self.dags, "harbour_eod.py"))
        self.env = dict(
            os.environ,
            AIRFLOW_HOME=self.home,
            AIRFLOW__CORE__DAGS_FOLDER=self.dags,
            AIRFLOW__CORE__LOAD_EXAMPLES="False",
            AIRFLOW__LOGGING__LOGGING_LEVEL="INFO",
            HARBOUR_FILE_WAIT_SECONDS=str(wait_seconds),
            HARBOUR_LANDING=str(eod.landing()),
            HARBOUR_DB=dsn(),
            PYTHONUNBUFFERED="1",
        )
        out = subprocess.run([sys.executable, "-m", "airflow", "db", "migrate"], env=self.env, capture_output=True, text=True, timeout=300)
        if out.returncode:
            raise RuntimeError("airflow db migrate failed:\n" + (out.stdout + out.stderr)[-2000:])

    def run(self, date: str, timeout: int = 420):
        p = subprocess.run([sys.executable, "-m", "airflow", "dags", "test", DAG_ID, date], env=self.env, capture_output=True, text=True, timeout=timeout)
        text = p.stdout + p.stderr
        lines = [l for l in text.splitlines() if l.strip()]
        keep = [l for l in lines if any(k in l for k in ("Error", "error", "Exception", "loaded ", "published position", "control file", "Traceback", "DagRun Finished"))]
        return p.returncode, "\n".join((keep or lines)[-8:])

    def cleanup(self):
        shutil.rmtree(self.home, ignore_errors=True)


def snapshot(d: str) -> dict:
    with psycopg.connect(dsn()) as c:
        stg = c.execute("SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM stg_npci_raw WHERE txn_date = %s", (d,)).fetchone()
        brk = c.execute("SELECT rrn, break_type FROM recon_breaks WHERE business_date = %s ORDER BY 1, 2", (d,)).fetchall()
        pos = c.execute("SELECT npci_net, gl_net, difference, breaks FROM treasury_position WHERE business_date = %s ORDER BY published_at", (d,)).fetchall()
    return {"stg": (stg[0], Decimal(stg[1])), "breaks": brk, "positions": [tuple(Decimal(x) if not isinstance(x, int) else x for x in r) for r in pos]}


def stg_dates() -> dict:
    with psycopg.connect(dsn()) as c:
        return {str(d): n for d, n in c.execute("SELECT txn_date, COUNT(*) FROM stg_npci_raw GROUP BY 1 ORDER BY 1").fetchall()}


def _paise(x) -> int:
    return int((Decimal(x) * 100).to_integral_value())


def run(dag_file: str, verbose: bool = True, wait_seconds: int = 90, late_after: float = 20) -> Result:
    say = (lambda m: print(m, flush=True)) if verbose else (lambda m: None)
    res = Result()
    if not os.path.exists(dag_file):
        raise SystemExit(f"No DAG at {dag_file}")
    eod.reset()
    say("▶ Preparing Airflow (fresh metadata database)")
    af = Airflow(dag_file, wait_seconds)
    try:
        def go(label, d):
            t0 = time.time()
            code, tail = af.run(d)
            res.runs.append((label, d, code, tail))
            say(f"    {label:<22} {d}  {'succeeded' if code == 0 else 'FAILED'} in {time.time() - t0:.0f}s")
            return code

        say("▶ Backfill 13 to 16 Oct")
        codes = {}
        for d in eod.DATES:
            feeder = None
            if d == eod.LATE_DATE:
                feeder = threading.Timer(late_after, eod.land, args=(d, "full"))
                feeder.start()
            codes[d] = go("backfill", d)
            if feeder:
                feeder.join()
                eod.land(d, "full")
        after_backfill = {d: snapshot(d) for d in eod.DATES}
        dates_after_backfill = stg_dates()

        say("▶ On-call reruns 15 and 16 Oct")
        rerun_codes = {d: go("rerun", d) for d in ("2026-10-15", eod.LATE_DATE)}
        after_rerun = {d: snapshot(d) for d in eod.DATES}

        say("▶ NPCI sends a corrected 14 Oct file; rerun 14 Oct")
        eod.land(eod.SHORT_DATE, "full")
        corrected_code = go("rerun after correction", eod.SHORT_DATE)
        final = {d: snapshot(d) for d in eod.DATES}
    finally:
        af.cleanup()

    exp = {d: eod.expected(d) for d in eod.DATES}
    C = res.controls

    counts = {d: len(final[d]["positions"]) for d in eod.DATES}
    C.append(Control("One published position per business date", all(n == 1 for n in counts.values()),
                     ", ".join(f"{d[-5:]}: {n}" for d, n in counts.items()) + " row(s) in treasury_position"))

    wrong = []
    for d in eod.DATES:
        rows = final[d]["positions"]
        if not rows:
            wrong.append(f"{d[-5:]} not published")
            continue
        npci, gl, diff, brk = rows[-1]
        e = exp[d]
        if (_paise(npci), _paise(gl), _paise(diff), brk) != (e["npci_net"], e["gl_net"], e["difference"], e["breaks"]):
            wrong.append(f"{d[-5:]} shows difference {fmt_inr(_paise(diff))} with {brk} breaks; expected {fmt_inr(e['difference'])} with {e['breaks']}")
    C.append(Control("Published figures agree with NPCI and the ledger", not wrong,
                     "13 to 15 Oct agree; 16 Oct shows Friday's ₹61,250.00 difference with its 2 breaks" if not wrong else "; ".join(wrong[:3])))

    changed = [d[-5:] for d in ("2026-10-15", eod.LATE_DATE) if after_rerun[d] != after_backfill[d]]
    C.append(Control("A rerun changes nothing", not changed and all(c == 0 for c in rerun_codes.values()),
                     "15 and 16 Oct identical after rerunning" if not changed and all(c == 0 for c in rerun_codes.values())
                     else (f"rerun changed {', '.join(changed)}: " + "; ".join(f"{d[-5:]} staging rows {after_backfill[d]['stg'][0]:,}→{after_rerun[d]['stg'][0]:,}, positions {len(after_backfill[d]['positions'])}→{len(after_rerun[d]['positions'])}" for d in ("2026-10-15", eod.LATE_DATE) if d[-5:] in changed)) if changed
                     else "a rerun failed: " + ", ".join(d for d, c in rerun_codes.items() if c)))

    late = after_backfill[eod.LATE_DATE]
    n_late, want = late["stg"][0], exp[eod.LATE_DATE]["records"]
    late_ok = codes[eod.LATE_DATE] == 0 and n_late == want
    if late_ok:
        late_detail = f"16 Oct run waited for NPCI's control file and loaded all {want:,} records"
    elif codes[eod.LATE_DATE] != 0:
        late_detail = f"16 Oct's first run failed instead of waiting for the file to finish arriving ({n_late:,} of {want:,} records staged)"
    elif n_late < want:
        late_detail = f"16 Oct's first run read the file before NPCI finished writing it: {n_late:,} of {want:,} records"
    else:
        late_detail = f"staging held {n_late:,} rows for 16 Oct against {want:,} records in NPCI's file"
    C.append(Control("Late file waited for, not read half-written", late_ok, late_detail))

    expected_dates = {d: exp[d]["records"] for d in eod.DATES if d != eod.SHORT_DATE}
    got = {d: n for d, n in dates_after_backfill.items() if d != eod.SHORT_DATE}
    stray = sorted(set(got) - set(expected_dates))
    off = [d for d in expected_dates if got.get(d) != expected_dates[d]]
    C.append(Control("Each run processed its own business date", not stray and not off,
                     "staging holds exactly 13, 15 and 16 Oct after the backfill" if not stray and not off
                     else "after the backfill, staging held " + ", ".join(f"{d}: {n:,}" for d, n in sorted(dates_after_backfill.items())) + (f" (dates no run asked for: {', '.join(stray)})" if stray else "")))

    pub_short = after_backfill[eod.SHORT_DATE]["positions"]
    held = codes[eod.SHORT_DATE] != 0 and not pub_short
    fixed = corrected_code == 0 and len(final[eod.SHORT_DATE]["positions"]) == 1
    if held and fixed:
        short_detail = "14 Oct failed its control check and published nothing; after NPCI's correction it published once"
    elif pub_short:
        short_detail = "14 Oct was published from a file one record short of its control totals"
    elif codes[eod.SHORT_DATE] == 0:
        short_detail = "the 14 Oct run reported success but never loaded or published 14 Oct"
    else:
        short_detail = "14 Oct was held, but the rerun after NPCI's correction did not publish it exactly once"
    C.append(Control("A file that fails its control totals is not published", held and fixed, short_detail))

    if verbose:
        for c in C:
            print(f"  {'PASS' if c.ok else 'FAIL'}  {c.name}: {c.detail}", flush=True)
        failed_runs = [r for r in res.runs if r[2] != 0 and not (r[1] == eod.SHORT_DATE and r[0] == "backfill")]
        if failed_runs:
            label, d, code, tail = failed_runs[0]
            print(f"\n    First unexpected failure ({label} {d}), last lines:\n      " + tail.replace("\n", "\n      "))
    return res
