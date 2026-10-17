"""Harbour Bank's data for value date 16 Oct 2026 (fictional, deterministic).
Same accounts, RRNs and amounts as ReadSigil Workshop 1. Amounts are integer paise."""
import json
from decimal import Decimal
from functools import lru_cache
from importlib import resources


@lru_cache(maxsize=1)
def load() -> dict:
    with resources.files("harbour").joinpath("data/harbour-2026-10-16.json").open("r", encoding="utf-8") as f:
        return json.load(f)


def rupees(paise: int) -> Decimal:
    return (Decimal(paise) / 100).quantize(Decimal("0.01"))


def fmt_inr(paise: int) -> str:
    """Indian digit grouping: 2,18,468.56"""
    neg = paise < 0
    s = f"{abs(paise) / 100:.2f}"
    whole, frac = s.split(".")
    if len(whole) > 3:
        head, tail = whole[:-3], whole[-3:]
        groups = []
        while len(head) > 2:
            groups.insert(0, head[-2:])
            head = head[:-2]
        if head:
            groups.insert(0, head)
        whole = ",".join(groups + [tail])
    return ("-" if neg else "") + "₹" + whole + "." + frac


def raw_by_account() -> dict:
    out = {}
    for r in load()["npci_raw"]:
        out[r["account_id"]] = out.get(r["account_id"], 0) + (r["amount"] if r["dr_cr"] == "CR" else -r["amount"])
    return out
