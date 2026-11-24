# Linewerk — Progress

> The mentor's memory. Updated at the end of every session with the block the mentor provides.
> Header sections are edited in place; the session log is append-only, **newest entry first**.

## Current position

- **Next session:** W1S4 — Data profiling (part 2): time and labels. Plan status: on track (W1S4 and W1F left this week)
- **Week:** 1 of 6 — Setup, Spark fundamentals, data profiling
- **Plan status:** on track
- **Started:** 2026-11-20 · **Target finish:** ~2026-12-31 (may stretch to 7 weeks over Christmas)

## Milestones

- [ ] **M1** — Environment works, data profiled, real numbers known (Week 1)
- [ ] **M2** — Long-format silver tables, partitioned and benchmarked (Week 2)
- [ ] **M3** — Star schema + SCD2 control limits + first findings (Week 3)
- [ ] **M4** — Live replay monitored against current limits (Week 4)
- [ ] **M5** — Same gold code scheduled on Databricks, CI green (Week 5)
- [ ] **M6** — Project published: dashboard, findings, README, final post (Week 6)

## Measured numbers

> Filled in as they are measured. These end up in the README and in interview answers. Never estimate here.

| Metric | Value | Measured in |
|---|---|---|
| Raw data size on disk (in-scope files) | 9.36 GiB unzipped (658 MiB zipped): numeric 1.99 GiB ×2, date 2.69 GiB ×2 | W1S2 |
| Parts (train / test) | train 1,183,747 / test 1,183,748 | W1S2 / W1S3 |
| Columns (numeric / date) | train_numeric 970 (Id + 968 features + Response), test_numeric 969 / date 1,157 (Id + 1,156) | W1S2 / W1S3 |
| Lines / station IDs | 4 lines, 52 station IDs (S0–S51), 50 with numeric features | W1S3 |
| Explicit schema vs inferSchema (train_numeric, read + count) | 1.77 s vs 5.64 s (~3.2×) | W1S2 |
| Non-null measurements (= silver rows) | train 216,741,930 / test 216,694,404 / total 433,436,334 (18.9% of wide cells) | W1S3 |
| Null count, 968 columns: single pass vs per-column loop | 15.0 s / 2 jobs vs ~24 min / ~1,936 jobs (loop measured on 20 columns, extrapolated) | W1S3 |
| Overall failure rate | — | — |
| Silver build time | — | — |
| Query speed-up after layout optimisation | — | — |
| Alarms: naive join vs point-in-time (SCD2) | — | — |
| Streaming throughput (events/s) and latency | — | — |

## Weak spots

> Concepts to revisit in recall checks until they are solid.

- Why Spark runs natively on macOS: Docker runs inside a Linux VM that reserves RAM; native leaves 8–10 GB for the driver
- Spark memory in local mode: spark.driver.memory is the whole budget; ~300 MB reserved, ~60% of the rest is the unified execution/storage pool; must be set before the JVM starts
- Driver and executor share one JVM in local[*]; tasks run in parallel as threads (one per core), not one after another
- Kafka bootstrap vs advertised.listeners: first connection works, then the client uses the advertised address from the metadata (Docker hostname trap)
- Job vs stage vs task: job = one action, stage = split at each shuffle, task = one per partition
- Reading physical plans in plain words: partial/final aggregate around one Exchange; broadcast join vs sort-merge join; hash partitioning (same key → same partition)
- `df.count()` (action, returns a number) vs `df.groupBy().count()` (transformation, returns a DataFrame)
- CSV column pruning: `count()` can skip parsing values; an aggregation over all columns cannot

## Parking lot

> Good ideas and tangents that are not for the current session. Reviewed on Flex Days.

- W5: spark.py master and config.py root detection both assume a local repo; add an env var override (e.g. LINEWERK_DATA_DIR) for Databricks
- Verify that the two 476-byte part-00000 files in the smoke table are empty schema-only files (optional)
- Why do date files compress 47× vs 7.5× for numeric? → check in W1S4 (timestamps constant per station?)
- Convention: Kafka topic names use dots only, never underscores (metric name collision warning)
- W5: branch protection on main — CI must pass before merge
- W2S3: try spark.sql.shuffle.partitions lower than 200 for a 8-core laptop and measure
- W1S4: check join completeness — does every numeric Id have a date row (and vice versa)?
- W2S1: CSV read mode for bronze — PERMISSIVE silently nulls malformed rows; consider FAILFAST or a corrupt-record column
- W1F: roadmap and project description say "51 stations" — data has 52 station IDs (S0–S51); fix in the changelog
- W2 (layout): L3 holds ~73% of filled cells — partitioning by line would be skewed; include this in the W2S3 benchmark

## Open questions

- —

---

## Session log

### 2026-11-24 — W1S3 — Shape and sparsity
- **Status:** done
- **Done:** `naming.py` (single regex with capture groups, shared by schema and parsing); `transforms/parsing.py` (`ColumnInfo` frozen dataclass, fail-fast `parse_column`) + `build_column_lookup` with tests. `jobs/profile_shape.py`: size / rows / columns for all 4 files; test headers = train headers minus `Response`. `quality/profiling.py`: `non_null_counts` — all 968 features in one aggregation; `tests/conftest.py` with a session-scoped SparkSession. `jobs/null_count_benchmark.py`: loop vs single pass with job groups. `quality/fill_ratio.py` + `jobs/non_null_profile.py`: fill ratio per station and line in plain Python from a JSON cache (invalidated by size + mtime). `docs/profiling.md` with files, column structure, sparsity, expected silver size, hypotheses H1–H4.
- **Not done / moved:** —
- **Learned:** `df.count()` is an action, `df.groupBy().count()` is a transformation that returns a DataFrame. `F.count(col)` counts only non-null values. One aggregation over 968 columns reads the file once (15 s), a loop of counts reads it once per column (~24 min); `count()` alone is faster still because CSV column pruning lets Spark skip parsing the values. Nulls here are structural ("part did not visit this station"), and column count is a poor proxy for volume (L1: 53% of features, ~6% of filled cells). `spark.ui.retainedJobs` (default 1000) caps what the status tracker remembers.
- **Struggled with:** The `groupBy().count()` vs `df.count()` trap in the recall check; forgot what CSV column pruning means; misread one toy `count(col)` result. First `parse_column` had an inverted condition and string-typed fields (type hints are not enforced at runtime); pydantic was used without being declared in `pyproject.toml`.
- **Decisions:** DEC-013, DEC-014
- **Interview Q:** What does 81% sparsity in a ~1,000-column table tell you about storage and modelling? → ok (long format and "null = not measured" right; add the measured numbers, schema-change / per-feature-logic argument, and a concrete layout example)
- **LinkedIn note:** Counting nulls in 968 columns: a per-column loop would need ~1,936 Spark jobs and ~24 min; one aggregation took 15 s and 2 jobs. And column count lied about volume: L1 has 53% of the features but ~6% of the filled cells.
- **Next:** W1S4 — Data profiling (part 2): time and labels

### 2026-11-23 — W1S2 (finish) — Schema strategy and reading Spark plans
- **Status:** done
- **Done:** Feature-branch workflow started (`w1s2-schema-explain`, PR). `ingest/schema.py`: explicit schema generated from the CSV header (Id/Response int, all features double, fail-fast on unknown columns). `jobs/schema_benchmark.py`: explicit vs inferSchema with warm-up. Read physical plans for filter, groupBy-count and a join (broadcast vs sort-merge) and matched them to jobs/stages/tasks in the Spark UI.
- **Not done / moved:** — (join on the full tables comes in W2S2 as planned)
- **Learned:** Explicit schema needs no extra pass; inferSchema reads the whole file once more and its types depend on the rows it sees (19 int columns on a sample, 16 on the full file). A job is one action, stages are split at shuffles, tasks are one per partition; the number of read partitions is min(128 MB, file size / cores). `groupBy().count()` counts inside each partition first, then shuffles only the small partial results. A join either copies the small side to every task (broadcast) or shuffles and sorts both sides by the key (sort-merge join).
- **Struggled with:** Too many new terms at once in the explain block (HashAggregate, Exchange, AQE, codegen) — asked to slow down; plain-language explanations with the "8 shift supervisors" analogy worked. Mentor rule from now on: max 1–2 new terms per step, plain words first. Mixed up jobs and tasks in the first prediction.
- **Decisions:** DEC-011, DEC-012
- **Interview Q:** What causes a shuffle in Spark, and why is it expensive? → ok (wide ops and cost items right; state "same key must meet in one partition", disk not memory, stage boundary)
- **LinkedIn note:** inferSchema typed 19 columns as int on a 100k sample but only 16 on the full file — the same column changes type depending on the rows Spark sees.
- **Next:** W1S3 — Data profiling (part 1): shape and sparsity

### 2026-11-22 — W1S1 (finish) + W1S2 (part 1) — Kafka up, data downloaded
- **Status:** partially done (W1S1 done; W1S2 partially done)
- **Done:** Single-node Kafka 4.3.2 (KRaft, combined broker+controller) in Docker; topic `linewerk.smoke` (3 partitions) created, listed and described → W1S1 closed. Kaggle API token set up in `~/.kaggle` (outside the repo, `chmod 600`); `kaggle` + `six` added as dev dependencies (kaggle imports six without declaring it). `config.py` with repo-root-based `RAW_DIR`. `ingest/download.py`: idempotent download + unzip of the 4 in-scope files via the Kaggle Python API, temp dir + atomic rename, `.done` markers, failures propagate. Tested from a clean state and re-run (all skipped).
- **Not done / moved:** inferSchema vs explicit schema timing, select/filter/groupBy/join practice, `explain()` and Spark UI shuffle → W1S2 (continued).
- **Learned:** Kafka keeps messages after they are read (retention); consumers track their own offsets, which is what makes replay possible. Bootstrap is only the entry point: the broker returns metadata with `advertised.listeners`, and the client then connects to those addresses directly. Spark reads `.gz` (a codec) but not `.zip` (an archive), and gzip is not splittable, so raw files are stored as plain CSV. A rename on the same filesystem is atomic, so temp dir + move never leaves a half-written file.
- **Struggled with:** Many new Kafka terms at once (broker, controller, ISR, bootstrap) — a "now vs W4" split helped. Mixed up the Databricks outbound restriction with the `advertised.listeners` problem.
- **Decisions:** DEC-009, DEC-010
- **Interview Q:** How do you make a batch ingestion job idempotent? → good (clear step-by-step; add the one-line definition, "atomic rename", failure propagation, and the table-level answer)
- **LinkedIn note:** The date files are 59 MiB zipped but 2.69 GiB unzipped (47×), while the numeric files only grow 7.5× — the timestamps are so repetitive that the compression ratio already hints at how the line records time.
- **Next:** W1S2 (continued) — schema strategy, `explain()` and the Spark UI

### 2026-11-21 — W1S1 (part 2) — Java, pinned Spark + Delta, SparkSession factory, Delta smoke test
- **Status:** partially done
- **Done:** Temurin JDK 17 with `JAVA_HOME`; Python 3.12 pinned; `pyproject.toml` with `pyspark==4.1.1` and `delta-spark==4.3.1`, `uv.lock` committed; `spark.py` (`get_spark()` with Delta extensions and configurable driver memory); `jobs/smoke.py` (overwrite + append, read back, history) and `make smoke` working.
- **Not done / moved:** Kafka (KRaft) in Docker + topic listing → first ~45 min of the next session.
- **Learned:** Spark is a JVM engine controlled from Python via Py4J; Delta is a plugin (jar) inside it, so versions must match — the real limit is in the package metadata (`pyspark<=4.1.1`), not only in the compatibility table, and the resolver caught it at install time. The Delta reader trusts the log, not the folder: 7 Parquet files on disk, 5 referenced by `add` actions, `count()` = 5. One non-empty partition = one task = one file. `overwrite` + `append` is idempotent in content but history keeps growing.
- **Struggled with:** Too many new terms at once (JVM, Catalyst, Scala binary version) — asked to separate "needed now" from "later", which worked. Imported `src.linewerk` instead of `linewerk`, which hid a typo in the wheel package path (`stc/linewerk`); found it once the import was fixed.
- **Decisions:** DEC-008
- **Interview Q:** Driver vs executor — what runs where in `local[*]`, and what does it mean for memory? → ok (roles correct; missed that both live in one JVM and run in parallel, and the driver-memory-is-the-whole-budget point)
- **LinkedIn note:** My first Delta table had 7 Parquet files on disk but only 5 belonged to the table — the transaction log, not the folder, decides what the table is.
- **Next:** W1S1 (finish: Kafka) → W1S2 — Spark DataFrame fundamentals and lazy evaluation

### 2026-11-20 — W1S1 (part 1) — Repository skeleton
- **Status:** partially done
- **Done:** GitHub repo `linewerk` created; folder layout (`src/linewerk/` with `__init__.py` packages, `tests/`, `databricks/`, `docs/`); `.gitignore`; README stub; ROADMAP / PROGRESS / DECISIONS moved into `docs/`; repo connected to the Claude project and synced.
- **Not done / moved:** Java 17, `uv` project with pinned pyspark + delta-spark, `spark.py`, Delta smoke test, Kafka in Docker → W1S1 (continued), next session.
- **Learned:** Git tracks files, not folders, so empty folders need a placeholder; in Python packages that is `__init__.py`. The `src/` layout forces tests to import the installed package. In `.gitignore`, a leading `/` anchors a pattern to the repo root and a trailing `/` matches directories only.
- **Struggled with:** Mixed up Free Edition limits with paid-trial credit limits (recall check).
- **Decisions:** —
- **Interview Q:** How do you make sure raw data never ends up in a public repo, including by accident? → ok (anchoring correct; missing defence in depth: tracked files, history rewrite, pre-commit/CI checks)
- **LinkedIn note:** Day 1 of Linewerk: the first line of `.gitignore` is a licence decision — the Bosch data must never be redistributed, so the repo ships a download script instead of data.
- **Next:** W1S1 (continued) — PySpark + Delta smoke test and Kafka (KRaft) running locally

### 2026-11-19 — W0 — Project planning
- **Status:** done
- **Done:** Chose the dataset (Bosch Production Line Performance), defined the quality-team story and the four business questions, designed the architecture, wrote `ROADMAP.md` v1.0, mentor instructions, and the `PROGRESS.md` / `DECISIONS.md` templates.
- **Not done / moved:** —
- **Learned:** Databricks Free Edition is serverless-only and supports streaming only with `AvailableNow`, so Kafka and continuous streaming stay local. Control limits derived from data need versioning, which is a natural SCD2 use case.
- **Struggled with:** —
- **Decisions:** DEC-001 … DEC-007
- **Interview Q:** —
- **LinkedIn note:** The project started from a real gap in my CV: Spark, streaming and lakehouse experience.
- **Next:** W1S1 — Project skeleton and environment
