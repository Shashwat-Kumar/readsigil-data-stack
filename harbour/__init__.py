"""Harbour Bank (fictional): shared data, schema and checks for the ReadSigil data-stack workshops."""
import os

TOPIC = "upi.settled"
DLQ = "upi.settled.dlq"
GROUP = "harbour-posting"
PARTITIONS = 6
VALUE_DATE = "2026-10-16"
GL_UPI = "GL-UPI-STL"


def bootstrap() -> str:
    return os.environ.get("HARBOUR_KAFKA", "localhost:9092")


def dsn() -> str:
    return os.environ.get("HARBOUR_DB", "postgresql://harbour:harbour@localhost:5432/harbour")


def consumer_config(**overrides) -> dict:
    """Settings every Harbour consumer starts from. Short session timeouts let a
    restarted consumer get its partitions back in seconds instead of 45."""
    cfg = {
        "bootstrap.servers": bootstrap(),
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "session.timeout.ms": 6000,
        "heartbeat.interval.ms": 1500,
        "max.poll.interval.ms": 60000,
    }
    cfg.update(overrides)
    return cfg


def producer_config(**overrides) -> dict:
    cfg = {
        "bootstrap.servers": bootstrap(),
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 5,
        "partitioner": "murmur2_random",  # same key-to-partition mapping as Java clients
    }
    cfg.update(overrides)
    return cfg
