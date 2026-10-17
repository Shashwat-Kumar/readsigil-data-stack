"""Kafka housekeeping for the workshops: topics, publishing, lag and the dead-letter topic."""
import time
import uuid

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from . import DLQ, GROUP, PARTITIONS, TOPIC, bootstrap, producer_config
from .events import stream


def admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap()})


def wait_ready(timeout: float = 60) -> None:
    end = time.time() + timeout
    err = None
    while time.time() < end:
        try:
            admin().list_topics(timeout=5)
            return
        except KafkaException as e:
            err = e
            time.sleep(1)
    raise RuntimeError(f"Kafka is not reachable at {bootstrap()}: {err}. Is `docker compose up -d --wait` running?")


def reset_topics() -> None:
    a = admin()
    try:
        a.delete_consumer_groups([GROUP])[GROUP].result(15)
    except Exception:
        pass  # group did not exist
    existing = set(a.list_topics(timeout=10).topics)
    doomed = [t for t in (TOPIC, DLQ) if t in existing]
    if doomed:
        for f in a.delete_topics(doomed, operation_timeout=30).values():
            f.result()
        end = time.time() + 60
        while time.time() < end and set(doomed) & set(a.list_topics(timeout=10).topics):
            time.sleep(0.5)
    for _ in range(30):
        try:
            futs = a.create_topics([NewTopic(TOPIC, PARTITIONS, 1), NewTopic(DLQ, 1, 1)], operation_timeout=30)
            for f in futs.values():
                f.result()
            break
        except KafkaException as e:
            if "already exists" in str(e) or "marked for deletion" in str(e).lower():
                time.sleep(1)
                continue
            raise
    end = time.time() + 30
    while time.time() < end:
        md = a.list_topics(timeout=10).topics
        if TOPIC in md and len(md[TOPIC].partitions) == PARTITIONS and DLQ in md:
            return
        time.sleep(0.3)
    raise RuntimeError("Topics were not created in time")


def publish() -> int:
    p = Producer(producer_config())
    n = 0
    for key, value in stream():
        p.produce(TOPIC, key=key.encode(), value=value, headers=[("source", b"upi-switch"), ("schema", b"1")])
        n += 1
        p.poll(0)
    remaining = p.flush(30)
    if remaining:
        raise RuntimeError(f"{remaining} events were not delivered to Kafka")
    return n


def _probe(group: str) -> Consumer:
    return Consumer({"bootstrap.servers": bootstrap(), "group.id": group, "enable.auto.commit": False})


def lag() -> dict:
    """{partition: (committed, end)} for the posting consumer group."""
    c = _probe(GROUP)
    try:
        parts = [TopicPartition(TOPIC, p) for p in range(PARTITIONS)]
        committed = c.committed(parts, timeout=10)
        out = {}
        for tp in committed:
            lo, hi = c.get_watermark_offsets(TopicPartition(TOPIC, tp.partition), timeout=10)
            out[tp.partition] = (max(tp.offset, 0) if tp.offset >= 0 else 0, hi)
        return out
    finally:
        c.close()


def total_lag() -> int:
    return sum(hi - c for c, hi in lag().values())


def read_dlq(timeout: float = 5) -> list:
    """Every record in the dead-letter topic: [(value, headers_dict)]."""
    c = Consumer({"bootstrap.servers": bootstrap(), "group.id": f"harbour-dlq-reader-{uuid.uuid4()}", "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    try:
        lo, hi = c.get_watermark_offsets(TopicPartition(DLQ, 0), timeout=10)
        if hi <= lo:
            return []
        c.assign([TopicPartition(DLQ, 0, lo)])
        out = []
        end = time.time() + timeout
        while len(out) < hi - lo and time.time() < end:
            m = c.poll(0.5)
            if m is None or m.error():
                continue
            out.append((m.value(), {k: (v.decode(errors="replace") if v else "") for k, v in (m.headers() or [])}))
        return out
    finally:
        c.close()
