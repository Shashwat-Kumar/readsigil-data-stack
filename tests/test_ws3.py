"""Workshop 3 must teach what it claims: the reference DAG passes every control on real
Airflow, the production DAG fails, and each deliberate bug is caught by the control that
names it. A real scheduler backfill (airflow standalone) must produce the same result.
Needs Postgres (`docker compose up -d --wait postgres`) and Airflow (scripts/install-airflow.sh)."""
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import pytest

from harbour import eod
from harbour.checks import ws3

ROOT = Path(__file__).resolve().parents[1]
WS = ROOT / "workshops" / "03-airflow"
SOLUTION = (WS / "solution" / "harbour_eod.py").read_text()
NAMES = {
    "one": "One published position per business date",
    "figures": "Published figures agree with NPCI and the ledger",
    "rerun": "A rerun changes nothing",
    "late": "Late file waited for, not read half-written",
    "date": "Each run processed its own business date",
    "gate": "A file that fails its control totals is not published",
}


def controls(r):
    return {c.name: c for c in r.controls}


def report(r, label):
    gha = os.environ.get("GITHUB_ACTIONS") == "true"
    for c in r.controls:
        line = f"[{label}] {'PASS' if c.ok else 'FAIL'} {c.name}: {c.detail}"
        print(line)
        if gha:
            print(f"::notice title=ws3 {label}::{line[:900]}", flush=True)


def mutant(tmp_path, name, replacements):
    src = SOLUTION
    for old, new in replacements:
        assert old in src, f"mutant {name}: pattern not found: {old!r}"
        src = src.replace(old, new)
    p = tmp_path / f"{name}.py"
    p.write_text(src)
    return str(p)


def test_solution_passes_every_control():
    r = ws3.run(str(WS / "solution" / "harbour_eod.py"))
    report(r, "solution")
    assert r.ok, "; ".join(f"{c.name}: {c.detail}" for c in r.controls if not c.ok)


def test_production_dag_fails():
    r = ws3.run(str(WS / "dags" / "harbour_eod.py"))
    report(r, "production DAG")
    c = controls(r)
    assert not c[NAMES["date"]].ok and not c[NAMES["one"]].ok


def test_wall_clock_date_is_caught(tmp_path):
    path = mutant(tmp_path, "wall_clock", [
        ("import csv\n", "import csv\nfrom datetime import date\n"),
        ("(ds=None):\n", "(ds=None):\n        ds = (date.today() - timedelta(days=1)).isoformat()\n"),
    ])
    r = ws3.run(path)
    report(r, "wall clock")
    assert not controls(r)[NAMES["date"]].ok


def test_appending_on_rerun_is_caught(tmp_path):
    path = mutant(tmp_path, "append", [('            cur.execute("DELETE FROM stg_npci_raw WHERE txn_date = %s", (ds,))\n', "")])
    r = ws3.run(path)
    report(r, "append on rerun")
    assert not controls(r)[NAMES["rerun"]].ok


def test_waiting_for_the_csv_instead_of_the_control_file_is_caught(tmp_path):
    path = mutant(tmp_path, "csv_sensor", [("eod.control_path(ds).exists()", "eod.csv_path(ds).exists()")])
    r = ws3.run(path)
    report(r, "sensor on CSV")
    assert not controls(r)[NAMES["late"]].ok


def test_publishing_without_the_control_check_is_caught(tmp_path):
    path = mutant(tmp_path, "no_gate", [("load_npci_raw() >> check_control_totals() >> reconcile()", "load_npci_raw() >> reconcile()")])
    r = ws3.run(path)
    report(r, "no control check")
    assert not controls(r)[NAMES["gate"]].ok


def test_real_scheduler_backfill_gives_the_same_result():
    """The path the guide teaches: airflow standalone, unpause, airflow backfill create."""
    eod.reset()
    home = tempfile.mkdtemp(prefix="harbour-standalone-")
    dags = Path(home) / "dags"
    dags.mkdir()
    (dags / "harbour_eod.py").write_text(SOLUTION)
    env = dict(os.environ, AIRFLOW_HOME=home, AIRFLOW__CORE__DAGS_FOLDER=str(dags), AIRFLOW__CORE__LOAD_EXAMPLES="False",
               AIRFLOW__API__PORT="8091", AIRFLOW__CORE__EXECUTION_API_SERVER_URL="http://localhost:8091/execution/",
               HARBOUR_FILE_WAIT_SECONDS="120", HARBOUR_LANDING=str(eod.landing()))
    af = lambda *a, **kw: subprocess.run([sys.executable, "-m", "airflow", *a], env=env, capture_output=True, text=True, timeout=kw.get("timeout", 120))
    log = open(Path(home) / "standalone.log", "w")
    proc = subprocess.Popen([sys.executable, "-m", "airflow", "standalone"], env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    try:
        deadline = time.time() + 240
        while time.time() < deadline and "harbour_eod" not in af("dags", "list").stdout:
            time.sleep(3)
        assert "harbour_eod" in af("dags", "list").stdout, "Airflow never registered the DAG"
        assert af("dags", "unpause", "harbour_eod").returncode == 0
        out = af("backfill", "create", "--dag-id", "harbour_eod", "--from-date", "2026-10-13", "--to-date", "2026-10-16")
        assert out.returncode == 0, out.stdout[-1500:] + out.stderr[-1500:]
        threading.Timer(25, eod.land, args=(eod.LATE_DATE, "full")).start()
        deadline = time.time() + 420
        states = {}
        while time.time() < deadline:
            out = af("dags", "list-runs", "harbour_eod", "-o", "json").stdout
            payload = next((l for l in out.splitlines() if l.startswith("[")), "[]")  # Airflow logs to stdout before the JSON
            try:
                states = {r["logical_date"][:10]: r["state"] for r in json.loads(payload) if r["run_id"].startswith("backfill__")}
            except ValueError:
                states = {}
            if len(states) == 4 and all(s in ("success", "failed") for s in states.values()):
                break
            time.sleep(5)
        if os.environ.get("GITHUB_ACTIONS") == "true":
            print(f"::notice title=ws3 scheduler backfill::run states {states}", flush=True)
        assert states == {"2026-10-13": "success", "2026-10-14": "failed", "2026-10-15": "success", "2026-10-16": "success"}, states
        for d in ("2026-10-13", "2026-10-15", "2026-10-16"):
            snap, e = ws3.snapshot(d), eod.expected(d)
            assert len(snap["positions"]) == 1, (d, snap["positions"])
            npci, gl, diff, brk = snap["positions"][0]
            assert (ws3._paise(npci), ws3._paise(gl), ws3._paise(diff), brk) == (e["npci_net"], e["gl_net"], e["difference"], e["breaks"]), d
        assert not ws3.snapshot(eod.SHORT_DATE)["positions"], "14 Oct must not be published from the short file"
    finally:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(30)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
        log.close()
