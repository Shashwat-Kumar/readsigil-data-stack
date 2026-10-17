"""Workshop 2 checks: run a posting consumer the way production would, then audit the result.

Two scenarios, each from a clean ledger and a freshly published stream:
  crash  one consumer, killed with SIGKILL three times part-way through, restarted each time
  scale  three consumers in the same group at once

After each, six controls are audited against the ledger, NPCI's raw file and Kafka itself."""
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field

from .. import DLQ, TOPIC, db, kafka_ops
from ..data import fmt_inr, load, raw_by_account
from ..events import POISON, valid_rrns

TARGET_ROWS = 2 * len(load()["npci_raw"])


@dataclass
class Control:
    name: str
    ok: bool
    detail: str


@dataclass
class Run:
    scenario: str
    controls: list = field(default_factory=list)
    notes: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return bool(self.controls) and all(c.ok for c in self.controls)


class Fleet:
    """Starts, kills and restarts consumer processes, keeping their output for diagnosis."""

    def __init__(self, path: str, size: int):
        self.path, self.size = path, size
        self.procs = [None] * size
        self.logs = [tempfile.NamedTemporaryFile("w+", suffix=f"-consumer{i}.log", delete=False) for i in range(size)]
        self.exits = []  # (slot, code, last_lines)

    def start(self, i: int) -> None:
        env = dict(os.environ, PYTHONUNBUFFERED="1")
        self.procs[i] = subprocess.Popen([sys.executable, self.path], stdout=self.logs[i], stderr=subprocess.STDOUT, env=env)

    def start_all(self) -> None:
        for i in range(self.size):
            self.start(i)

    def kill(self, i: int) -> None:
        p = self.procs[i]
        if p and p.poll() is None:
            p.send_signal(signal.SIGKILL)
            p.wait(10)

    def reap(self) -> list:
        """Slots whose process exited on its own since the last call."""
        died = []
        for i, p in enumerate(self.procs):
            if p is not None and p.poll() is not None:
                self.exits.append((i, p.returncode, self.tail(i)))
                self.procs[i] = None
                died.append(i)
        return died

    def tail(self, i: int, n: int = 6) -> str:
        self.logs[i].flush()
        with open(self.logs[i].name, encoding="utf-8", errors="replace") as f:
            lines = [l.rstrip() for l in f.readlines() if l.strip()]
        return "\n".join(lines[-n:])

    def stop(self) -> None:
        for p in self.procs:
            if p and p.poll() is None:
                p.send_signal(signal.SIGTERM)
        for i, p in enumerate(self.procs):
            if p and p.poll() is None:
                try:
                    p.wait(8)
                except subprocess.TimeoutExpired:
                    p.kill()
                    p.wait(5)


def _prepare() -> None:
    db.reset()
    kafka_ops.reset_topics()
    kafka_ops.publish()


def _drive(fleet: Fleet, conn, kills: list, timeout: float, log) -> None:
    """Run the fleet until it has consumed everything and gone quiet, or until timeout.
    kills: fractions of TARGET_ROWS at which slot 0 is SIGKILLed and restarted."""
    fleet.start_all()
    kills = sorted(kills)
    start = time.time()
    last_rows, last_change, last_lag_check, lag = -1, time.time(), 0.0, None
    self_exits_without_progress = 0
    progress_at_last_exit = -1
    while time.time() - start < timeout:
        time.sleep(0.1)
        rows = db.upi_rows(conn)
        if rows != last_rows:
            last_rows, last_change = rows, time.time()
        if kills and rows >= kills[0] * TARGET_ROWS:
            kills.pop(0)
            fleet.kill(0)
            log(f"SIGKILL at {rows:,} of {TARGET_ROWS:,} rows; restarting")
            fleet.start(0)
            continue
        for i in fleet.reap():
            code = fleet.exits[-1][1]
            log(f"consumer {i + 1} exited by itself (code {code}) at {rows:,} rows; restarting")
            if rows == progress_at_last_exit:
                self_exits_without_progress += 1
            else:
                self_exits_without_progress = 0
            progress_at_last_exit = rows
            if self_exits_without_progress >= 3:
                log("consumer keeps exiting without posting anything new; giving up")
                return
            fleet.start(i)
        if time.time() - last_lag_check > 1.0:
            last_lag_check = time.time()
            try:
                lag = kafka_ops.total_lag()
            except Exception:
                lag = None
        if lag == 0 and time.time() - last_change > 3 and not kills:
            return
    log(f"stopped after {timeout:.0f}s without the group catching up")


def _controls(conn, scenario: str) -> list:
    d = load()
    raw = {r["rrn"]: r for r in d["npci_raw"]}
    rows = db.upi_postings(conn)
    cust = [r for r in rows if r[2] == "CUST"]
    by_ref = {}
    for r in rows:
        by_ref.setdefault(r[1], []).append(r)
    out = []

    posted = {r[1] for r in cust}
    missing = [x for x in raw if x not in posted]
    unknown = [x for x in posted if x not in raw]
    out.append(Control(
        "Every settled transaction posted exactly once",
        not missing and not unknown and len(cust) == len(raw),
        f"{len(posted):,} of {len(raw):,} RRNs posted" + (f"; {len(missing):,} missing, e.g. {missing[0]}" if missing else "") + (f"; {len(unknown)} unknown references" if unknown else ""),
    ))

    bad_legs = []
    for ref, legs in by_ref.items():
        c = [l for l in legs if l[2] == "CUST"]
        g = [l for l in legs if l[2] == "GL"]
        if len(c) != 1 or len(g) != 1 or c[0][4] == g[0][4] or c[0][5] != g[0][5] or g[0][3] != "GL-UPI-STL":
            bad_legs.append(ref)
    out.append(Control(
        "Both legs, opposite sides, same amount",
        not bad_legs,
        "every posting has its GL-UPI-STL leg" if not bad_legs else f"{len(bad_legs):,} reference{'s' if len(bad_legs) != 1 else ''} without a matching GL leg, e.g. {bad_legs[0]}",
    ))

    want = raw_by_account()
    got = {}
    for r in cust:
        got[r[3]] = got.get(r[3], 0) + (r[5] if r[4] == "CR" else -r[5])
    breaks = [a for a in set(want) | set(got) if want.get(a, 0) != got.get(a, 0)]
    gl = db.gl_net(conn)
    net = d["npci_net_settlement"]["net_amount"]
    out.append(Control(
        "Ledger agrees with NPCI",
        not breaks and gl == net,
        f"240 accounts and the settlement GL agree ({fmt_inr(-net)} payable)" if not breaks and gl == net
        else f"{len(breaks)} account{'s' if len(breaks) != 1 else ''} differ{'' if len(breaks) != 1 else 's'}; settlement GL {fmt_inr(-gl)} payable vs NPCI {fmt_inr(-net)}",
    ))

    dlq = kafka_ops.read_dlq()
    poison_in = [v for v, h in dlq if v == POISON]
    good = valid_rrns()
    wrongly_parked = []
    for v, h in dlq:
        if v == POISON:
            continue
        try:
            if json.loads(v).get("rrn") in good:
                wrongly_parked.append(v)
        except Exception:
            wrongly_parked.append(v)
    if poison_in and not wrongly_parked:
        dlq_detail = f"{len(poison_in)} cop{'y' if len(poison_in) == 1 else 'ies'} in {DLQ}"
        if len(poison_in) > 1:
            dlq_detail += " (a retry can park it twice; dedupe on the source-offset header)"
    elif wrongly_parked:
        dlq_detail = f"{len(wrongly_parked)} valid event{'s were' if len(wrongly_parked) != 1 else ' was'} parked instead of posted"
    else:
        dlq_detail = f"the malformed message is not in {DLQ}" + (f" ({len(dlq)} other records are)" if dlq else "")
    out.append(Control("Malformed message parked in the dead-letter topic", bool(poison_in) and not wrongly_parked, dlq_detail))

    try:
        lag = kafka_ops.lag()
        behind = {p: hi - c for p, (c, hi) in lag.items() if hi - c > 0}
        out.append(Control(
            "Consumer group caught up and offsets committed",
            not behind,
            f"lag 0 on all {len(lag)} partitions of {TOPIC}" if not behind else "behind on " + ", ".join(f"partition {p}: {n:,}" for p, n in sorted(behind.items())),
        ))
    except Exception as e:
        out.append(Control("Consumer group caught up and offsets committed", False, f"could not read offsets: {e}"))

    order_breaks = []
    seen = {}
    for r in cust:
        t = raw.get(r[1], {}).get("settled_at")
        if t is None:
            continue
        prev = seen.get(r[3])
        if prev and t < prev:
            order_breaks.append(r[3])
        seen[r[3]] = t
    out.append(Control(
        "Each account's transactions posted in settlement order",
        not order_breaks,
        "order preserved within every account" if not order_breaks else f"{len(set(order_breaks))} accounts posted out of order, e.g. {order_breaks[0]}",
    ))
    return out


def run(consumer: str, scenarios=("crash", "scale"), verbose=True, timeout: float = 180) -> list:
    if not os.path.exists(consumer):
        raise SystemExit(f"No consumer at {consumer}")
    kafka_ops.wait_ready()
    log = (lambda m: print(f"    {m}", flush=True)) if verbose else (lambda m: None)
    results = []
    for sc in scenarios:
        if verbose:
            print(f"\n▶ {'Crash test: one consumer, killed three times' if sc == 'crash' else 'Scale-out: three consumers in one group'}", flush=True)
        _prepare()
        fleet = Fleet(consumer, 1 if sc == "crash" else 3)
        run_ = Run(sc)
        with db.connect(autocommit=True) as conn:
            try:
                _drive(fleet, conn, [0.2, 0.5, 0.8] if sc == "crash" else [], timeout, lambda m: (run_.notes.append(m), log(m)))
            finally:
                fleet.stop()
            run_.controls = _controls(conn, sc)
        crashes = [e for e in fleet.exits if e[1] not in (0, -9, -15)]
        if crashes and verbose:
            print("    last output from a consumer that stopped by itself:")
            print("      " + crashes[-1][2].replace("\n", "\n      "))
        run_.notes.extend(e[2] for e in crashes[-1:])
        results.append(run_)
        if verbose:
            for c in run_.controls:
                print(f"  {'PASS' if c.ok else 'FAIL'}  {c.name}: {c.detail}", flush=True)
    return results
