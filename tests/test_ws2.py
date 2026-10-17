"""Workshop 2 must teach what it claims: the reference consumer passes every control under
crashes and scale-out against real Kafka, and each of the starter's bugs is caught by the
control that names it. Needs `docker compose up -d --wait`."""
from pathlib import Path

import pytest

from harbour.checks import ws2

ROOT = Path(__file__).resolve().parents[1]
WS = ROOT / "workshops" / "02-kafka"
SOLUTION = (WS / "solution" / "consumer.py").read_text()
NAMES = {
    "once": "Every settled transaction posted exactly once",
    "legs": "Both legs, opposite sides, same amount",
    "npci": "Ledger agrees with NPCI",
    "dlq": "Malformed message parked in the dead-letter topic",
    "lag": "Consumer group caught up and offsets committed",
    "order": "Each account's transactions posted in settlement order",
}


def controls(results):
    return {c.name: c for r in results for c in r.controls}


def mutant(tmp_path, name, replacements):
    src = SOLUTION
    for old, new in replacements:
        assert old in src, f"mutant {name}: pattern not found: {old!r}"
        src = src.replace(old, new)
    p = tmp_path / f"{name}.py"
    p.write_text(src)
    return str(p)


def report(results):
    for r in results:
        for c in r.controls:
            print(f"[{r.scenario}] {'PASS' if c.ok else 'FAIL'} {c.name}: {c.detail}")


def test_solution_passes_every_control_under_crashes_and_scale_out():
    results = ws2.run(str(WS / "solution" / "consumer.py"), verbose=True)
    report(results)
    failed = [f"[{r.scenario}] {c.name}: {c.detail}" for r in results for c in r.controls if not c.ok]
    assert not failed, "; ".join(failed)


def test_starter_loses_postings():
    results = ws2.run(str(WS / "consumer.py"), scenarios=("crash",), verbose=True, timeout=120)
    report(results)
    c = controls(results)
    assert not c[NAMES["once"]].ok, "the starter should lose postings; the crash test did not catch it"
    assert not c[NAMES["npci"]].ok


def test_without_on_conflict_a_redelivery_stops_the_consumer(tmp_path):
    path = mutant(tmp_path, "no_on_conflict", [("ON CONFLICT (txn_ref, leg) DO NOTHING\n", "")])
    results = ws2.run(path, scenarios=("crash",), verbose=True, timeout=120)
    report(results)
    c = controls(results)
    assert not c[NAMES["once"]].ok
    assert not c[NAMES["lag"]].ok
    assert any("UniqueViolation" in n or "duplicate key" in n for r in results for n in r.notes), "expected the duplicate-key error in the consumer's output"


def test_committing_before_posting_loses_events_on_a_crash(tmp_path):
    path = mutant(tmp_path, "commit_first", [
        ("            msg = consumer.poll(1.0)\n            if msg is None:\n                continue\n",
         "            msg = consumer.poll(1.0)\n            if msg is None:\n                continue\n            if not msg.error():\n                consumer.commit(message=msg, asynchronous=False)  # acknowledge first\n                import time; time.sleep(0.02)  # e.g. an enrichment call between acknowledging and posting\n"),
        ("            consumer.commit(message=msg, asynchronous=False)  # only after the ledger has it\n", ""),
    ])
    results = ws2.run(path, scenarios=("crash",), verbose=True, timeout=150)
    report(results)
    assert not controls(results)[NAMES["once"]].ok, "committing before posting should lose events when the consumer is killed"


def test_skipping_bad_messages_without_a_dead_letter_fails_the_dlq_control(tmp_path):
    path = mutant(tmp_path, "no_dlq", [("                park(producer, msg, str(err))\n", "")])
    results = ws2.run(path, scenarios=("crash",), verbose=True, timeout=120)
    report(results)
    c = controls(results)
    assert not c[NAMES["dlq"]].ok
    assert c[NAMES["once"]].ok and c[NAMES["npci"]].ok, "only the dead-letter control should fail"
