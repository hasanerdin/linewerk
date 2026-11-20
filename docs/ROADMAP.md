# Linewerk — Roadmap

> A streaming + batch lakehouse that replays Bosch's 51-station production line data through Kafka,
> monitors every measurement against versioned SPC control limits, and tells a quality team
> **where failures originate and which parts are at risk before final inspection.**

This roadmap is a living document. It is reviewed every Flex Day and changed when reality disagrees with it.
Changes are logged at the bottom (see *Roadmap changelog*).

---

## 1. The story

**Setting.** A plant with 4 production lines and 51 stations. Every part is measured and tested as it moves
along the line. Roughly 0.6% of parts fail final quality control — but they are only caught at the very end,
after every station has already spent time and material on them.

**The quality team asks:**

1. **Where do failures concentrate?** Which stations and which flow paths (the sequence of stations a part visits) have the highest failure rates?
2. **Which measurements drift out of statistical control**, and when?
3. **How predictive is an out-of-control measurement** at station X of a final failure? (lift, not ML)
4. **Can we flag at-risk parts while they are still on the line**, before final inspection?

**How the data maps to the story.**

| Dataset part | Role in the story |
|---|---|
| Train set (labelled with `Response`) | **Historical production** — used to compute baselines, control limits and findings |
| Test set (no labels) | **Live production** — replayed through Kafka in timestamp order and monitored in near real time |

**Why SCD2 is genuinely needed.** Measurements are anonymised, so we have no engineering tolerances.
Like real SPC, control limits are derived from the data (mean ± 3σ) and **re-baselined when the process shifts**.
Limits therefore have versions (`valid_from` / `valid_to` / `is_current`). A measurement must be judged against
the limit **valid at the time it was taken** — judging an old measurement against today's limit produces false alarms.
That is `dim_control_limit` as an SCD Type 2 dimension with a point-in-time join.

---

## 2. Final deliverables

- **Line Quality Dashboard** (Databricks AI/BI): yield by line and station, failure Pareto, control charts, at-risk parts
- **Findings report**: a `Findings` section in the README plus a LinkedIn article, with interpretable, numbered results
- **Repository**: tested, documented, one-command local run, CI green, measured numbers in the README
- **5–6 LinkedIn posts**, one per week

## 3. Scope

**In scope:** train + test `numeric` and `date` files · PySpark batch · Delta Lake medallion · Kafka (local, Docker, KRaft) ·
Spark Structured Streaming · SPC + SCD2 · star schema · Databricks Free Edition (Auto Loader, Jobs, Asset Bundles, dashboard) ·
GitHub Actions CI.

**Out of scope (on purpose):** ML models, FastAPI, Streamlit, cloud infrastructure (AWS/Azure/Terraform).
These are already proven in Zephyrwerk and Mobility Delay.

**Stretch goals (only if ahead of plan):** `categorical` files · Western Electric rules 2–4 · Tableau Public view on aggregated gold data ·
short demo GIF of the live stream.

## 4. Constraints and ground rules

- **Cost: €0.** No credit card trials (Confluent Cloud, paid Databricks, cloud accounts). Kafka runs locally.
- **Hardware:** MacBook M3, 16 GB RAM, ~80 GB free disk. PySpark runs **natively**, only Kafka runs in Docker. Spark driver memory 8–10 GB.
- **Data licence:** Kaggle competition rules apply. **Raw data is never committed.** The repo contains only a download script.
- **Databricks Free Edition limits:** serverless only; streaming only with `Trigger.AvailableNow()`; restricted outbound internet
  (it cannot reach local Kafka); one SQL warehouse (2X-Small); max 5 concurrent job tasks. Data reaches Databricks by uploading files to a Unity Catalog Volume.
- **Hasan writes the core code.** The mentor provides boilerplate only (see Project Instructions).
- **Language:** all repository files, docs and posts in English.

## 5. Architecture

```
                         ┌──────────────────────────── LOCAL (MacBook) ────────────────────────────┐
 Kaggle CSV (train/test) │  Spark batch ──► bronze (wide Delta) ──► silver (long Delta) ──► gold    │
                         │                                                │                        │
                         │  Replay producer (test set, ts-ordered, speed-up, optional chaos)        │
                         │        │                                                                 │
                         │        ▼                                                                 │
                         │  Kafka (Docker, KRaft) ──► Structured Streaming                          │
                         │                           (watermark, dedup, checkpoint, foreachBatch)   │
                         │                                   │ uses current control limits (SCD2)   │
                         │                                   ▼                                      │
                         │                    live_measurements · spc_alerts · at_risk_parts        │
                         └───────────────────────────────────┬──────────────────────────────────────┘
                                                             │ Databricks SDK upload (Parquet)
                                                             ▼
                         ┌──────────────────── DATABRICKS FREE EDITION ────────────────────┐
                         │ UC Volume ──► Auto Loader (AvailableNow) ──► gold (same code)    │
                         │ Jobs defined in Asset Bundles · AI/BI dashboard                  │
                         └──────────────────────────────────────────────────────────────────┘
```

**Key design principle: one transformation library, two runtimes, two modes.**
The functions in `src/linewerk/transforms/` are pure DataFrame-in / DataFrame-out. Batch and streaming call the same
functions, and the same gold code runs locally and on Databricks. This is a deliberate talking point.

### Target data model

| Layer | Table | Grain |
|---|---|---|
| Bronze | `bronze_numeric`, `bronze_date` | one row per part (wide, as delivered) + ingestion metadata |
| Silver | `silver_measurements` | one row per part × feature (long): `part_id, line, station, feature, value, ts, split` |
| Silver | `silver_parts` | one row per part: `part_id, response, split` |
| Silver | `silver_station_visits` | one row per part × station: first/last timestamp |
| Gold | `dim_station`, `dim_feature`, `dim_flow_path`, `dim_part`, `dim_time_bucket` | dimensions |
| Gold | `dim_control_limit` (**SCD2**) | one row per feature × limit version |
| Gold | `fact_measurement` | one row per part × feature, with `limit_version_key` and `is_out_of_control` |
| Gold | `agg_station_quality`, `agg_flow_path_quality`, `agg_ooc_lift` | report-ready aggregates |
| Streaming | `live_measurements`, `spc_alerts`, `at_risk_parts` | live monitoring outputs |

### Suggested repository layout

```
linewerk/
├── docs/            ROADMAP.md · PROGRESS.md · DECISIONS.md · findings.md · architecture.png
├── src/linewerk/
│   ├── config.py    spark.py (SparkSession factory)
│   ├── ingest/      download + bronze
│   ├── transforms/  pure functions: parsing, unpivot, spc, scd2, gold
│   ├── quality/     data quality checks
│   ├── streaming/   producer.py · consumer.py
│   └── jobs/        entry points (batch_bronze, batch_silver, batch_gold, stream_monitor, upload)
├── tests/           unit tests with small fixtures + local SparkSession
├── databricks/      bundle (databricks.yml, job definitions, dashboard)
├── docker-compose.yml · pyproject.toml · Makefile · README.md
└── data/            (git-ignored) raw/ · lake/ · checkpoints/
```

---

## 6. Timeline

- **Start:** Friday 20 November 2026
- **Rhythm:** 4 core sessions (~5 h each) + 1 Flex Day per week ≈ 20–25 h/week
- **Duration:** 6 weeks (~130 h including learning time). Target finish: ~31 December 2026
- **Holidays:** Christmas falls in Weeks 5–6. If a week off is taken, the plan shifts to 7 weeks — that is fine and expected.

| Week | Dates (approx.) | Theme | Milestone |
|---|---|---|---|
| 1 | 20 Nov – 26 Nov | Setup, Spark fundamentals, data profiling | **M1:** environment works, data profiled, real numbers known |
| 2 | 27 Nov – 3 Dec | Bronze + silver (batch) | **M2:** long-format silver tables, partitioned and benchmarked |
| 3 | 4 Dec – 10 Dec | Gold, SPC, SCD2, first findings | **M3:** star schema + versioned limits + first findings |
| 4 | 11 Dec – 17 Dec | Kafka + Structured Streaming | **M4:** live replay monitored against current limits |
| 5 | 18 Dec – 24 Dec | Databricks + automation + CI | **M5:** same gold code scheduled on Databricks, CI green |
| 6 | 25 Dec – 31 Dec | Dashboard, findings, docs, **half buffer** | **M6:** project published |

Session IDs follow `W<week>S<session>` (e.g. `W2S3`). Flex Days are `W<week>F`.

Every session has: **Goal · Concepts to learn · Tasks · Done when · Interview questions**.
"Done when" is the acceptance test — the session is not finished until it is true or explicitly moved to the Parking lot.

---

## Week 1 — Setup, Spark fundamentals, data profiling

**Week outcome:** a working local stack, a first feel for Spark, and *real* measured facts about the data.

### W1S1 — Project skeleton and environment
- **Goal:** a reproducible local environment where PySpark + Delta run and a Kafka container starts.
- **Concepts:** how Spark runs locally (driver, executors, `local[*]`); why Spark needs a JVM; Delta Lake = Parquet + transaction log; version compatibility between Spark and delta-spark.
- **Tasks:**
  1. Create the GitHub repo `linewerk`, the folder layout above, `.gitignore` (`data/`, checkpoints, `.venv`).
  2. Install Java 17; create the Python project with `uv`; pin `pyspark` and a **matching** `delta-spark` version (check the Delta compatibility matrix).
  3. Write `spark.py`: a SparkSession factory with Delta extensions and configurable driver memory.
  4. Smoke test: write a tiny DataFrame as Delta, read it back, inspect `_delta_log/`.
  5. `docker-compose.yml` with a single-node Kafka (KRaft); start it, create and list a test topic.
  6. Connect the repo to the Claude project (GitHub integration, `src/ tests/ docs/` + config files).
- **Done when:** `make smoke` (or equivalent) writes and reads a Delta table; Kafka topic listing works; first commit pushed.
- **Interview questions:** *What is in a Delta transaction log and what does it give you over plain Parquet?* · *Driver vs executor — what runs where in `local[*]` mode?*

### W1S2 — Spark DataFrame fundamentals and lazy evaluation
- **Goal:** be comfortable reading Spark plans and knowing *when* work actually happens.
- **Concepts:** transformations vs actions; lazy evaluation; narrow vs wide transformations; shuffles; partitions; `explain()`; the Spark UI (jobs, stages, tasks).
- **Tasks:**
  1. Download the Bosch data with the Kaggle CLI via a script (`ingest/download.py`); accept the competition rules first.
  2. Read one CSV with a **small sample** first; try `inferSchema` vs an explicit schema and time both.
  3. Practise: select, filter, groupBy, join on a sample; read the physical plan with `explain()`; open the Spark UI and identify a shuffle.
  4. Write down what surprised you in `PROGRESS.md`.
- **Done when:** data is downloaded; you can explain one `explain()` output in your own words; schema strategy decided.
- **Interview questions:** *Why is `inferSchema` expensive on large CSVs?* · *What causes a shuffle and why is it costly?*

### W1S3 — Data profiling (part 1): shape and sparsity
- **Goal:** know the data's real size, shape and sparsity — the numbers that will go in the README and interviews.
- **Concepts:** profiling a wide table efficiently; generating a schema programmatically from the header; null counting at scale without one action per column.
- **Tasks:**
  1. Build the schema from the CSV header (all features as double, `Id` as int, `Response` as int).
  2. For each file: file size on disk, row count, column count.
  3. Null ratio per feature, per station and per line — computed in a **single pass**.
  4. Count non-null measurements = the expected size of `silver_measurements`.
  5. Parse feature names (`L<line>_S<station>_F<feature>`, `..._D<n>`) into a lookup table.
- **Done when:** a profiling notebook/script outputs a summary table (sizes, rows, columns, sparsity, expected long-format row count) saved to `docs/profiling.md`.
- **Interview questions:** *How would you count nulls in 1,000 columns without 1,000 Spark jobs?* · *What does sparsity tell you about the right storage layout?*

### W1S4 — Data profiling (part 2): time and labels
- **Goal:** understand the timestamp columns and the label distribution well enough to design replay and SPC.
- **Concepts:** how date columns relate to numeric features (verify the `D` ↔ `F` mapping yourself — the source description suggests an offset); anonymised time units; class imbalance.
- **Tasks:**
  1. Verify the mapping between `D` columns and `F` features on real rows.
  2. Time range of train vs test; distribution of timestamps; are timestamps constant within a station?
  3. Estimate what one time unit means (document the reasoning, mark it as an assumption).
  4. Label distribution: failure rate overall and per line.
  5. Decide the "time bucket" for SPC baselines (e.g. a production-day equivalent) and record it in `DECISIONS.md`.
- **Done when:** `docs/profiling.md` has a time and label section; first two decisions recorded in `DECISIONS.md`.
- **Interview questions:** *How do you handle anonymised or unknown time units?* · *How does a 0.6% positive rate change how you evaluate results?*

### W1F — Flex Day: retro + LinkedIn post #1
- Retro: what took longer than planned? Update the roadmap if needed.
- **Post #1 — "Why I'm building Linewerk":** the story, the dataset (4 lines, 51 stations, thousands of columns), the real numbers from profiling, what will come next.
- Catch up on anything unfinished.

---

## Week 2 — Bronze and silver (batch)

**Week outcome:** clean, long-format silver tables with a deliberate physical layout and measured performance.

### W2S1 — Bronze ingestion
- **Goal:** raw CSVs land as Delta tables, unchanged in content, with lineage metadata.
- **Concepts:** what bronze is for (replayability, auditability); ingestion metadata; idempotent writes (overwrite vs append, `replaceWhere`); Delta table history and time travel.
- **Tasks:**
  1. `jobs/batch_bronze.py`: CSV → `bronze_numeric`, `bronze_date` with `_source_file`, `_ingested_at`, `split`.
  2. Make the job idempotent: running it twice gives the same table.
  3. Inspect `DESCRIBE HISTORY`; query an older version with time travel.
  4. Monitor memory in the Spark UI; tune driver memory / shuffle partitions if needed.
- **Done when:** bronze tables exist for train + test; row counts match profiling; job is idempotent.
- **Interview questions:** *Why keep a bronze layer at all?* · *How do you make a batch ingestion job idempotent?*

### W2S2 — Wide → long: the core silver transformation
- **Goal:** turn ~1,000 sparse columns into a long measurement table.
- **Concepts:** `unpivot`/`melt` vs `stack()`; exploding and the row-count explosion; dropping nulls early; joining numeric values to their timestamps.
- **Tasks:**
  1. Write `transforms/unpivot.py` as a **pure function** (DataFrame in → DataFrame out).
  2. Write `transforms/parsing.py`: feature name → `line, station, feature`.
  3. Join each measurement with its timestamp (using the mapping verified in W1S4).
  4. First unit tests with a tiny hand-made fixture (3 parts × 4 columns).
  5. Run on a subset first (one line), then measure time and memory.
- **Done when:** unit tests pass; `silver_measurements` built for at least one line; row count matches the profiling expectation.
- **Interview questions:** *How do you unpivot 1,000 columns in Spark, and what happens to the row count?* · *Why test transformations as pure functions?*

### W2S3 — Physical layout and performance
- **Goal:** make silver fast to query and justify every layout decision with a measurement.
- **Concepts:** partitioning (and over-partitioning); the small-file problem; file sizing; `OPTIMIZE`; Z-ordering / liquid clustering; data skew; Adaptive Query Execution.
- **Tasks:**
  1. Run the full silver build for train + test.
  2. Benchmark a fixed set of 3 typical queries (e.g. one station's measurements over time; one part's full history; failure rate by station).
  3. Try 2–3 layouts (no partitioning / by `line` / by `station` + Z-order on `feature`) and compare query time and file counts.
  4. Pick one, record the numbers and reasoning in `DECISIONS.md`.
- **Done when:** a before/after benchmark table exists in `docs/` and the chosen layout is applied.
- **Interview questions:** *When does partitioning hurt instead of help?* · *What is the small-file problem and how do you fix it in Delta?*

### W2S4 — Remaining silver tables and data quality
- **Goal:** complete silver and make bad data visible instead of silent.
- **Concepts:** data quality dimensions (completeness, uniqueness, validity); Delta `CHECK` constraints; quarantine pattern vs fail-fast.
- **Tasks:**
  1. Build `silver_parts` and `silver_station_visits` (first/last timestamp per part × station).
  2. Derive each part's **flow path** (ordered list of stations) — needed in Week 3.
  3. `quality/checks.py`: uniqueness of `(part_id, feature)`, non-null keys, finite values, timestamps within expected range.
  4. Add at least one Delta `CHECK` constraint; decide fail-fast vs quarantine and record it.
  5. Wire bronze → silver into one command (`make silver`).
- **Done when:** all silver tables built; quality checks run and report; one-command rebuild works.
- **Interview questions:** *Fail the pipeline or quarantine the bad rows — how do you decide?* · *What data quality checks would you put on a measurement table?*

### W2F — Flex Day: retro + LinkedIn post #2
- **Post #2 — "Turning a 1,000-column sparse table into a long-format lakehouse table":** the unpivot, the row-count change, the layout benchmark numbers.
- Retro and roadmap adjustment.

---

## Week 3 — Gold, SPC, SCD2 and first findings

**Week outcome:** a star schema with versioned control limits and the first interpretable results.

### W3S1 — Dimensional model
- **Goal:** design and build the star schema.
- **Concepts:** facts vs dimensions; grain; surrogate vs natural keys; star vs snowflake; conformed dimensions.
- **Tasks:**
  1. Write the grain of every gold table in one sentence each (in `DECISIONS.md`).
  2. Build `dim_station`, `dim_feature`, `dim_flow_path`, `dim_part`, `dim_time_bucket`.
  3. Build a first `fact_measurement` (without limits yet).
  4. Draw the model (simple diagram for the README).
- **Done when:** dimensions and the first fact table exist; grain documented; diagram drafted.
- **Interview questions:** *Define "grain" and why it is the first design decision.* · *Surrogate keys vs natural keys — when and why?*

### W3S2 — SPC control limits
- **Goal:** compute statistically sound control limits per feature.
- **Concepts:** SPC basics: centre line, ±3σ, in-control vs out-of-control; Western Electric rule 1; baseline windows; why limits come from in-control data.
- **Tasks:**
  1. `transforms/spc.py`: per feature and time bucket compute `n, mean, stddev`.
  2. Define the **re-baselining rule**: a new limit version starts only when the process mean shifts meaningfully (e.g. more than a chosen fraction of σ, with minimum sample size). Record the rule and its parameters in `DECISIONS.md`.
  3. Produce the sequence of limit versions per feature (`valid_from`, `valid_to`).
  4. Sanity-check a few features with a quick control chart plot.
- **Done when:** a versioned limits DataFrame exists and the rule is justified with at least one plotted example.
- **Interview questions:** *How would you set control limits without engineering tolerances?* · *Why not simply recompute limits every day?*

### W3S3 — SCD2 with Delta MERGE and point-in-time joins
- **Goal:** maintain `dim_control_limit` as a real SCD Type 2 table and use it correctly.
- **Concepts:** SCD types 0/1/2; `MERGE INTO` with matched/not-matched clauses; closing old versions; `is_current`; idempotency of MERGE; point-in-time (as-of) joins.
- **Tasks:**
  1. `transforms/scd2.py`: apply a new batch of limits to `dim_control_limit` using `MERGE` — close changed versions, insert new ones, leave unchanged ones alone.
  2. Test it: initial load, a run with changes, a re-run with the same input (must change nothing).
  3. Join `fact_measurement` to the limit **valid at the measurement's timestamp**; set `is_out_of_control`.
  4. Compare: how many alarms would a naive join to *current* limits produce? (This number goes in the README.)
- **Done when:** SCD2 tests pass; facts carry `limit_version_key`; naive-vs-correct alarm comparison computed.
- **Interview questions:** *Walk me through an SCD2 MERGE.* · *What goes wrong if you join facts to the current dimension version?*

### W3S4 — First findings
- **Goal:** answer the quality team's questions 1–3 with numbers.
- **Concepts:** Pareto analysis; rates with minimum support; lift (P(fail | OOC at station) / P(fail)); correlation vs causation.
- **Tasks:**
  1. `agg_station_quality`: failure rate per station and line; Pareto of failures.
  2. `agg_flow_path_quality`: failure rate per flow path (only paths with enough parts).
  3. `agg_ooc_lift`: lift of an out-of-control measurement at each station on final failure.
  4. Write the first draft of `docs/findings.md` — three findings, each with a number and a caveat.
- **Done when:** three findings written in plain language with supporting numbers.
- **Interview questions:** *How do you avoid misleading rates on small groups?* · *What would you tell a plant manager who reads your lift number as causation?*

### W3F — Flex Day: retro + LinkedIn post #3
- **Post #3 — "Why my control limits needed SCD Type 2":** the problem, the naive-vs-correct alarm numbers, one finding.
- Mid-project retro: honest check on pace; rescope Weeks 4–6 if needed.

---

## Week 4 — Kafka and Structured Streaming

**Week outcome:** the test set replays as a live stream and every measurement is checked against the current limits in near real time.

### W4S1 — Kafka fundamentals and the replay producer
- **Goal:** understand Kafka well enough to design topics deliberately, and publish a realistic event stream.
- **Concepts:** topics, partitions, offsets, consumer groups; message keys and per-key ordering; retention; delivery semantics (at-most/at-least/exactly-once); producer `acks` and idempotence.
- **Tasks:**
  1. Design the event: one message per part × station visit (part_id, line, station, ts, measurements). Write a JSON schema.
  2. Choose key (`part_id`) and partition count; record the reasoning.
  3. `streaming/producer.py`: read the test-set silver data in timestamp order and publish with a configurable **speed-up factor**.
  4. Consume a few messages with the CLI to verify.
- **Done when:** the producer streams the test set at a chosen speed; topic design documented.
- **Interview questions:** *How does Kafka guarantee ordering, and where does it stop guaranteeing it?* · *Why key by `part_id`?*

### W4S2 — Structured Streaming basics
- **Goal:** a streaming job that reads from Kafka and writes parsed events to Delta reliably.
- **Concepts:** micro-batches; triggers; sources and sinks; output modes; checkpoints and what they store; restart semantics.
- **Tasks:**
  1. `streaming/consumer.py`: read from Kafka, parse JSON with an explicit schema, reuse the **same** parsing/unpivot functions from `transforms/`.
  2. Write to `live_measurements` (Delta) with a checkpoint.
  3. Kill the job mid-stream and restart it: verify no gaps and no duplicates.
- **Done when:** a restart test passes and is documented.
- **Interview questions:** *What exactly is stored in a streaming checkpoint?* · *How does Spark achieve end-to-end exactly-once with Kafka and Delta?*

### W4S3 — Event time, watermarks, late and duplicate data
- **Goal:** handle the messy parts of streaming on purpose.
- **Concepts:** event time vs processing time; watermarks; late data; stateful operations and state size; `dropDuplicatesWithinWatermark`; windowed aggregations.
- **Tasks:**
  1. Add a **chaos mode** to the producer: configurable % of duplicates and late events.
  2. Add a watermark and deduplication to the consumer.
  3. A windowed aggregation: OOC count per station per time window.
  4. Show what happens to late events inside vs outside the watermark; record the numbers.
- **Done when:** with chaos mode on, output contains no duplicates and late-data behaviour is measured and explained.
- **Interview questions:** *What is a watermark and what trade-off does it make?* · *How do you deduplicate a stream without unbounded state?*

### W4S4 — Live SPC monitoring
- **Goal:** answer question 4 — flag at-risk parts while they are still on the line.
- **Concepts:** stream-static joins; `foreachBatch`; MERGE from a stream (upserts); refreshing the static side.
- **Tasks:**
  1. Join live measurements to the **current** control limits (`is_current = true`) → `spc_alerts`.
  2. Use `foreachBatch` + MERGE to maintain `at_risk_parts` (risk = OOC count across stations, weighted by the lift from W3S4).
  3. End-to-end run: producer → Kafka → consumer → alerts, with a short metrics summary (events/sec, latency).
- **Done when:** an end-to-end live run produces alerts and an at-risk list; throughput and latency measured.
- **Interview questions:** *When would you use `foreachBatch`?* · *How do you keep the static side of a stream-static join fresh?*

### W4F — Flex Day: retro + LinkedIn post #4
- **Post #4 — "Replaying a real factory line through Kafka":** topic design, chaos mode, watermark results, live alert numbers.

---

## Week 5 — Databricks, automation and CI

**Week outcome:** the same gold code runs on Databricks on a schedule, deployed with Asset Bundles, with CI protecting `main`.

### W5S1 — Databricks Free Edition and Unity Catalog
- **Goal:** get oriented on Databricks and land data in a Volume.
- **Concepts:** workspace, Unity Catalog (catalog.schema.table), Volumes, serverless compute; Free Edition limits.
- **Tasks:**
  1. Create the Free Edition account (verify with LinkedIn for higher limits); create catalog/schema/volume.
  2. `jobs/upload.py`: upload silver (and stream output) as Parquet to the Volume with the Databricks SDK; make it incremental.
  3. Explore the uploaded data in a notebook.
- **Done when:** incremental upload works; data visible in Unity Catalog.
- **Interview questions:** *What does Unity Catalog add over a Hive metastore?* · *What is a Volume and when do you use it instead of a table?*

### W5S2 — Auto Loader and gold on Databricks
- **Goal:** incremental ingestion on Databricks and the **same** gold code running there.
- **Concepts:** Auto Loader (`cloudFiles`), schema inference/evolution, `Trigger.AvailableNow()`; packaging code as a wheel; environment-specific config.
- **Tasks:**
  1. Auto Loader from the Volume into Databricks silver tables with `AvailableNow`.
  2. Package `src/linewerk` as a wheel; run the gold transforms on Databricks unchanged.
  3. Compare results with the local gold tables (row counts, a few aggregates).
- **Done when:** gold tables on Databricks match local results.
- **Interview questions:** *Auto Loader vs `COPY INTO` vs plain `spark.read`?* · *How do you keep code portable between local Spark and Databricks?*

### W5S3 — Jobs and Databricks Asset Bundles
- **Goal:** infrastructure-as-code for Databricks.
- **Concepts:** Jobs, tasks, dependencies, schedules; Asset Bundles (`databricks.yml`, targets, variables); validate/deploy/run.
- **Tasks:**
  1. Define a job: Auto Loader → gold → aggregates, as dependent tasks, scheduled.
  2. `databricks bundle validate / deploy / run` from the CLI.
  3. Record why Asset Bundles instead of Terraform here.
- **Done when:** the job is deployed from the bundle and a scheduled run succeeds.
- **Interview questions:** *What problem do Asset Bundles solve?* · *How would you promote a bundle from dev to prod?*

### W5S4 — CI with GitHub Actions
- **Goal:** protect `main` with automated checks.
- **Concepts:** testing Spark code in CI (session fixtures, small data); linting; branch protection; secrets in CI.
- **Tasks:**
  1. GitHub Actions: `ruff` + `pytest` (local SparkSession, Java set up in the runner) on every PR.
  2. Add `databricks bundle validate` to CI.
  3. Branch protection on `main`; at least one PR merged through the full flow.
- **Done when:** CI green on a PR; `main` protected.
- **Interview questions:** *How do you test Spark transformations fast?* · *What belongs in CI vs CD for a data platform?*

### W5F — Flex Day: retro + LinkedIn post #5
- **Post #5 — "Same code, two runtimes: local Spark and Databricks":** portability, Auto Loader, Asset Bundles.

---

## Week 6 — Dashboard, findings, documentation (half buffer)

**Week outcome:** the project is presentable, explainable and published.

### W6S1 — Line Quality Dashboard
- **Goal:** turn gold tables into a dashboard a quality engineer would use.
- **Concepts:** dashboard design for operations (one question per chart); Databricks AI/BI dashboards and SQL warehouse limits.
- **Tasks:** yield by line/station · failure Pareto · control chart for selected features (with limit versions visible) · at-risk parts list · screenshots for the README.
- **Done when:** dashboard answers questions 1–4; screenshots saved.

### W6S2 — Findings report
- **Goal:** final, defensible findings.
- **Tasks:** finalise `docs/findings.md` (each finding: number, chart, caveat, recommended action); turn it into a LinkedIn article draft.
- **Done when:** findings reviewed against the gold tables; article draft ready.

### W6S3 — README, architecture and the "measured numbers" section
- **Goal:** a README that a recruiter understands in 30 seconds and an engineer respects in 5 minutes.
- **Tasks:** problem → solution → architecture diagram → **measured numbers** (data size, rows, throughput, latency, benchmark gains, naive-vs-SCD2 alarms) → how to run → design decisions (link `DECISIONS.md`) → what I would do next.
- **Done when:** a fresh clone can be run by following the README alone.

### W6S4 — Buffer + interview rehearsal
- Finish anything slipped. Then a **mock interview**: the mentor asks 10 questions across the whole project; weak answers become notes in `PROGRESS.md`.
- Update CV and LinkedIn "Projects" with Linewerk.

### W6F — Launch
- **Post #6 — final post** with the findings, dashboard screenshot and repo link. Pin the repo on GitHub.

---

## 7. Risk register

| Risk | Likelihood | Mitigation |
|---|---|---|
| Out of memory during unpivot (16 GB RAM) | Medium | Process per file and per line; drop nulls before exploding; tune shuffle partitions |
| Learning curve slower than planned | High | Flex Days absorb delays; Week 6 is half buffer; stretch goals are the first thing to drop |
| `D` ↔ `F` mapping or time units unclear | Medium | Verify in W1S4; document assumptions explicitly |
| Spark / Delta version mismatch | Medium | Pin versions in W1S1 from the compatibility matrix |
| Databricks Free Edition limits hit | Low–Medium | Keep gold small; LinkedIn verification; heavy processing stays local |
| Motivation dip mid-project | Medium | Weekly visible output (post), Week 3 already produces findings |
| Christmas | Certain | Allowed to stretch to 7 weeks |

## 8. Definition of done (project)

- [ ] One command runs the full local pipeline; the Databricks job runs on schedule
- [ ] `dim_control_limit` is SCD2 with tests; facts use point-in-time limits
- [ ] Live replay produces alerts and an at-risk list with measured throughput/latency
- [ ] Dashboard + `findings.md` answer the four questions
- [ ] README contains measured numbers; `DECISIONS.md` explains the key trade-offs
- [ ] CI green; `main` protected
- [ ] 5–6 LinkedIn posts published; CV updated

## Roadmap changelog

| Date | Change | Reason |
|---|---|---|
| 2026-11-19 | v1.0 created | Initial plan |
