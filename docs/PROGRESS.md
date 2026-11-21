# Linewerk — Progress

> The mentor's memory. Updated at the end of every session with the block the mentor provides.
> Header sections are edited in place; the session log is append-only, **newest entry first**.

## Current position

- **Next session:** W1S1 (finish: Kafka in Docker, ~45 min) → W1S2 — Spark DataFrame fundamentals and lazy evaluation
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
| Raw data size on disk (in-scope files) | — | — |
| Parts (train / test) | — | — |
| Columns (numeric / date) | — | — |
| Non-null measurements (= silver rows) | — | — |
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

## Parking lot

> Good ideas and tangents that are not for the current session. Reviewed on Flex Days.

- Data paths: compute from the repo root or an env var in config.py instead of relative paths (W1S2)
- spark.py hard-codes master("local[*]"); on Databricks the platform provides the session (W5)
- Verify that the two 476-byte part-00000 files in the smoke table are empty schema-only files (optional)

## Open questions

- —

## LinkedIn posts

| # | Week | Topic | Status | Link |
|---|---|---|---|---|
| 1 | W1 | Why I'm building Linewerk | planned | — |
| 2 | W2 | 1,000-column sparse table → long-format lakehouse | planned | — |
| 3 | W3 | Why my control limits needed SCD Type 2 | planned | — |
| 4 | W4 | Replaying a real factory line through Kafka | planned | — |
| 5 | W5 | Same code, two runtimes: local Spark and Databricks | planned | — |
| 6 | W6 | Final post: findings + dashboard | planned | — |

---

## Session log

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
