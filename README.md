# Harbour Bank data stack

Hands-on workshops from [ReadSigil](https://readsigil.com/build-lab/) on the open-source data stack a bank runs on. Harbour Bank and every figure are fictional. The software, the controls and the failures are real.

[![Workshop 2 (Kafka) end to end](https://github.com/shashwat-kumar/readsigil-data-stack/actions/workflows/ws2-kafka.yml/badge.svg)](https://github.com/shashwat-kumar/readsigil-data-stack/actions/workflows/ws2-kafka.yml)

| Workshop | Situation | Runs on |
|---|---|---|
| [1 · Postgres, DuckDB](https://readsigil.com/build-lab/close-the-books/) | UPI settlement does not tie to the ledger | Your browser |
| [2 · Kafka](workshops/02-kafka/) | Post every transaction once, through redeliveries, crashes and month-end volume | Codespaces or your laptop |
| 3 · Airflow | The end-of-day pipeline fails at 02:10; backfill without posting twice | In preparation |
| 4 · Iceberg | Restate the March book after a late correction | In preparation |
| 5 · dbt Core | One definition of CASA ratio across fourteen reports | In preparation |
| 6 · Spark | The EOD batch slows because one corporate account fills one partition | In preparation |
| 7 · Metabase | The MD's morning dashboard, built on reconciled numbers only | In preparation |
| 8 · Capstone | Run the whole end-of-day pipeline yourself | In preparation |

A workshop is listed as available only when its end-to-end tests pass against the real tools.

## Start

[Open in GitHub Codespaces](https://codespaces.new/shashwat-kumar/readsigil-data-stack), or on a laptop with Docker and Python 3.10+:

```bash
docker compose up -d --wait
pip install -e .
harbour doctor
```
