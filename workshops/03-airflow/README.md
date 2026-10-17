# Workshop 3 · Backfill the week. Publish each night once.

**Harbour Bank · Sunday 18 October, 08:30.** Treasury reads one table every morning: `treasury_position`, one row per business date, written by the end-of-day pipeline in Airflow. Since Tuesday's deploy that table cannot be trusted.

- Nothing came through for 13, 14 or 15 October.
- 16 October appears three times, with three different figures.
- On-call reran the pipeline at 02:40 on Saturday when NPCI's file was late. The 07:42 email from Finance in Workshop 1 is what followed.

You own the DAG. Backfill 13 to 16 October so that each date is published exactly once, correctly, however many times it is run. Then the check will tell Treasury whether the table can be trusted again.

| | |
|---|---|
| Time | 60 minutes |
| You need | A GitHub account (for a free Codespace), or Docker and Python 3.10 to 3.13 on your laptop |
| Runs | Apache Airflow 3.2 and Postgres 17, for real |
| Before this | [Workshop 1](https://readsigil.com/build-lab/close-the-books/) found 16 Oct's breaks; [Workshop 2](../02-kafka/) made posting exactly-once |

## 0. Start

**In a browser:** open the repository in a Codespace ([direct link](https://codespaces.new/shashwat-kumar/readsigil-data-stack)). Airflow is installed for you.

**On a laptop:**

```bash
git clone https://github.com/shashwat-kumar/readsigil-data-stack
cd readsigil-data-stack
docker compose up -d --wait postgres
./scripts/install-airflow.sh
```

Point Airflow at this workshop and start it:

```bash
export AIRFLOW_HOME=$PWD/.airflow
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/workshops/03-airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow standalone
```

Leave that terminal running and open a second one with the same three `export` lines. The Airflow UI is at http://localhost:8080 (in a Codespace, open the forwarded port 8080). `airflow standalone` prints the admin password when it starts.

## 1. Look at what NPCI sent

```bash
harbour ws3 reset
ls -l landing/
```

Each business date has a CSV and a control file (`.ctl`) with the record count and the credit and debit totals. NPCI writes the CSV first and the control file last. Two dates are not clean:

- **14 Oct.** The CSV is one record short of what its control file says.
- **16 Oct.** The CSV is still being written. There is no control file yet.

## 2. Run one night

```bash
airflow dags test harbour_eod 2026-10-13
harbour ws3 status
```

`airflow dags test` runs one real DAG run for the business date you give it. Check which date the run actually loaded. **What you see depends on today's date, and that is the bug.** The DAG works out "last night" from the clock when the file is parsed, not from the run.

## 3. Backfill the week the way operations would

```bash
airflow dags unpause harbour_eod
airflow backfill create --dag-id harbour_eod --from-date 2026-10-13 --to-date 2026-10-16
```

Watch the four runs in the UI, then run `harbour ws3 status` again.

## 4. Run the check

```bash
harbour check ws3
```

The check runs your DAG the way operations did last week, in three stages:

1. **Backfill.** It runs 13 to 16 Oct in order. 16 Oct's file completes 20 seconds after that run starts.
2. **On-call reruns.** It reruns 15 and 16 Oct.
3. **NPCI's correction.** NPCI delivers a corrected 14 Oct file, and the check reruns 14 Oct.

It then audits six controls:

| Control | What it proves |
|---|---|
| One published position per business date | Treasury sees one figure per night |
| Published figures agree with NPCI and the ledger | The figures are right, including 16 Oct's ₹61,250 difference and its 2 breaks |
| A rerun changes nothing | On-call can rerun any night without making it worse |
| Late file waited for, not read half-written | The pipeline waits for NPCI to finish |
| Each run processed its own business date | A backfill does what it says |
| A file that fails its control totals is not published | A short file stops the run, and the corrected file goes through |

The production DAG fails them all. Fix one at a time.

## 5. Take the business date from the run

Every task can receive the run's logical date as `ds`. In a backfill of 13 to 16 Oct, each run gets its own.

<details><summary>Hint</summary>

Delete the module-level `BUSINESS_DATE` line. Add `ds=None` to each task's parameters and use it wherever the code says `BUSINESS_DATE`. Airflow fills it in. Anything computed at the top of a DAG file is evaluated whenever the file is parsed, on whatever day that is.
</details>

## 6. Make every write repeatable

Run any night twice and look at `harbour ws3 status`. Each task appends, so a rerun doubles the staging rows, the breaks and the published position.

<details><summary>Hint</summary>

Write each date as a replacement. In each task, inside the same transaction, `DELETE ... WHERE txn_date = %s` (or `business_date`) and then insert. `psycopg.connect(...)` used as a context manager commits once at the end, so a failure halfway leaves the previous result in place. `treasury_position` has no primary key, so delete-then-insert is the safe pattern here. A primary key and `INSERT ... ON CONFLICT DO UPDATE` is the better long-term fix.
</details>

## 7. Wait for the file to be finished

The DAG reads the CSV as soon as the run starts. For 16 Oct that means reading a file NPCI has not finished writing.

<details><summary>Hint</summary>

Add a first task that waits. In Airflow 3, `@task.sensor(poke_interval=5, timeout=..., mode="poke")` on a function returning `PokeReturnValue(is_done=...)` keeps checking until the condition is true. Wait for the control file, not the CSV. The CSV exists from the first byte; the control file exists only once the CSV is complete. `harbour.eod.control_path(ds)` gives you its path.

To try it by hand, start a run for 16 Oct, then in another terminal run `harbour ws3 arrive 2026-10-16`.
</details>

## 8. Refuse a file that does not add up

14 Oct's CSV is one record short. Publishing it would put a wrong figure in front of Treasury with no warning.

<details><summary>Hint</summary>

Add a task between loading and reconciling. It compares the count and totals in `stg_npci_raw` with `harbour.eod.read_control(ds)`, and raises `airflow.sdk.exceptions.AirflowFailException` if they differ. That fails the run immediately, with no retries, so nothing downstream publishes. Use retries for failures that might pass on their own, such as a dropped connection, and never for data that is wrong.

When NPCI sends the corrected file, run `harbour ws3 correct` and rerun 14 Oct.
</details>

## 9. Rerun the week

```bash
airflow backfill create --dag-id harbour_eod --from-date 2026-10-13 --to-date 2026-10-16 --reprocess-behavior completed
```

`--reprocess-behavior completed` reruns dates that already succeeded. With the fixes in place, this changes nothing.

## 10. Pass the check

```bash
harbour check ws3
```

When every control passes, compare with [`solution/harbour_eod.py`](solution/harbour_eod.py). Look at what 16 Oct publishes: a ₹61,250.00 difference and two breaks. A pipeline like this would have flagged Friday's duplicate and the missing credit at about 02:40 on Saturday, before anyone sent an email.

## On a Cloudera estate

Many Cloudera estates still schedule end-of-day work with **Oozie** coordinators. Cloudera Data Engineering runs **Airflow**, so the same DAG ports across, with Spark jobs launched by the CDE operator. The rules in this workshop are the same either way:

- Take the business date from the run, never from the clock.
- Replace a date's output rather than adding to it.
- Wait for the trigger or control file.
- Fail closed when control totals disagree.

On Hive or Impala, "replace a date" usually means `INSERT OVERWRITE` of that date's partition.

## What this repository proves

On every change, [GitHub Actions](../../.github/workflows/ws3-airflow.yml) installs Airflow and starts Postgres. It then:

- runs the reference DAG through the backfill, the reruns and the correction, where all six controls must pass;
- runs the production DAG and four deliberately broken DAGs (wall-clock date, appending on rerun, waiting for the CSV instead of the control file, no control check), each of which must fail the control that names its bug;
- runs a real scheduler backfill with `airflow standalone` and `airflow backfill create`, which must publish the same figures.
