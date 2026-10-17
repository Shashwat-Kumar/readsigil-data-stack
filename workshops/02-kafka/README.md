# Workshop 2 · Post every transaction once. Prove it.

**Harbour Bank · Monday 19 October.** Friday's duplicate debit came from a batch posting job that retried after a timeout. Harbour is replacing it with an event-driven consumer: the UPI switch publishes every settled transaction to Kafka, and the consumer posts both legs to the core ledger.

Risk will approve go-live only if the consumer survives what the switch really sends. That means redelivered events, a message cut off mid-write, consumers killed half-way through a batch, and month-end volume spread across several consumers.

You have the first draft of the consumer and a check that runs it the way production will. Make the check pass.

| | |
|---|---|
| Time | 45 to 60 minutes |
| You need | A GitHub account (for a free Codespace), or Docker and Python 3.10+ on your laptop |
| Runs | Apache Kafka 4.1 (KRaft) and Postgres 17, for real |
| Before this | [Workshop 1](https://readsigil.com/build-lab/close-the-books/), which found the duplicate |

## 0. Start the stack

**In a browser:** open this repository in a Codespace ([direct link](https://codespaces.new/shashwat-kumar/readsigil-data-stack)). Kafka and Postgres start on their own. A GitHub Free account includes 120 Codespaces hours a month, which is about 60 hours on the 2-core machine this needs.

**On a laptop:**

```bash
git clone https://github.com/shashwat-kumar/readsigil-data-stack
cd readsigil-data-stack
docker compose up -d --wait
pip install -e .
```

Then check both services:

```bash
harbour doctor
```

## 1. See what the switch sends

```bash
harbour reset
harbour publish
```

`harbour publish` reports more messages than the 1,800 transactions NPCI settled. Look at the topic:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic upi.settled

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic upi.settled --from-beginning --max-messages 5 \
  --property print.key=true --property print.partition=true --property print.offset=true
```

Three things to notice:

- **The topic has six partitions, and the key is the account.** Every event for one account lands on the same partition, so it is read in the order it was settled.
- **Some RRNs appear twice.** The switch republishes an event when it does not get an acknowledgement in time. Kafka delivers at least once, and duplicates are normal.
- **One message is not valid JSON.** It was cut off mid-write.

## 2. Run the first draft

```bash
python workshops/02-kafka/consumer.py
```

It stops almost at once:

```
psycopg.errors.UniqueViolation: duplicate key value violates unique constraint "uq_posting_leg"
DETAIL:  Key (txn_ref, leg)=(628900398525, CUST) already exists.
```

Since Friday the ledger refuses a second posting for the same `(txn_ref, leg)`, so the first redelivered event stops the consumer. Now look at the damage:

```bash
harbour status
```

The ledger has no postings, yet consumer lag is 1,638, not 1,838. The draft told Kafka its first batch of 200 events was handled *before* writing it. The write then failed and rolled back. As far as the consumer group is concerned, those 200 transactions are done, and nobody will ever post them.

Before you fix that, run the check that Risk will use:

```bash
harbour check ws2
```

The check runs your consumer twice, from a clean ledger each time:

- **Crash test.** One consumer, killed with `SIGKILL` at 20%, 50% and 80% of the way through, and restarted each time.
- **Scale-out.** Three consumers in the same group at once.

It then audits six controls:

| Control | What it proves |
|---|---|
| Every settled transaction posted exactly once | No event lost, none posted twice |
| Both legs, opposite sides, same amount | Double entry holds for every posting |
| Ledger agrees with NPCI | All 240 accounts and the settlement GL tie to NPCI's file |
| Malformed message parked in the dead-letter topic | Bad data is quarantined, not dropped and not blocking |
| Consumer group caught up and offsets committed | Nothing left unread, and the group would restart cleanly |
| Each account's transactions posted in settlement order | Parallelism has not reordered an account's history |

The first draft fails most of them. Fix one thing at a time and rerun the check.

## 3. Make a redelivery harmless

A redelivered event should change nothing. The ledger already rejects the duplicate, so the consumer only needs to stop treating that as an error.

<details><summary>Hint</summary>

Postgres can skip a row that would break a unique constraint: `INSERT ... ON CONFLICT (txn_ref, leg) DO NOTHING`.
</details>

## 4. Acknowledge only what the ledger has

You saw this in step 2: the first draft calls `consumer.commit()` before the postings are written. Any failure in between, whether an error or a killed process, leaves Kafka believing those events were handled. The crash test shows this as missing RRNs.

<details><summary>Hint</summary>

Commit the offset after the database transaction commits. Then a crash means an event is delivered again, and step 3 makes the second delivery a no-op. At-least-once delivery plus an idempotent write gives you each transaction posted once.

Committing every message keeps the code simple. Committing every few hundred, after the batch's transaction commits, is faster and just as safe.
</details>

## 5. Park the bad message, keep going

One malformed message should not stop posting for every account behind it on that partition. It also must not disappear without a trace.

<details><summary>Hint</summary>

Catch the parse error, produce the original bytes to `upi.settled.dlq` with headers that say why it failed and where it came from (`source-partition`, `source-offset`), then commit and carry on. A crash between parking and committing can park the message twice, so whoever works the dead-letter topic dedupes on those headers.
</details>

## 6. Scale out for month end

Reset, publish, then start three consumers in three terminals:

```bash
harbour reset && harbour publish
python workshops/02-kafka/consumer.py      # in three terminals
harbour status                             # in a fourth: watch lag fall
```

Kafka splits the six partitions between the three consumers. A seventh consumer would sit idle: partitions are the unit of parallelism. Because the key is the account, each account is still handled by one consumer at a time, in order.

## 7. Pass the check

```bash
harbour check ws2
```

When every control passes, compare your consumer with [`solution/consumer.py`](solution/consumer.py).

## On a Cloudera estate

Cloudera's Streams Messaging is Apache Kafka, so consumer groups, offsets, partitions and the dead-letter pattern work exactly as here. In production you would add:

- **Streams Messaging Manager** to watch consumer lag.
- **Schema Registry** so a malformed message is rejected when it is produced, not discovered when it is consumed.
- **Ranger policies** on who can produce to `upi.settled` and who can read the dead-letter topic.

## What this repository proves

On every change, [GitHub Actions](../../.github/workflows/ws2-kafka.yml) starts real Kafka and Postgres and:

- runs the reference consumer through both scenarios, where all six controls must pass;
- runs the first draft and three deliberately broken consumers (no `ON CONFLICT`, committing before posting, skipping bad messages without parking them), each of which must fail the control that names its bug;
- repeats the check inside the dev container, so the Codespaces path is tested too.
