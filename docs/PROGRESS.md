# Linewerk — Progress

> The mentor's memory. Updated at the end of every session with the block the mentor provides.
> Header sections are edited in place; the session log is append-only, **newest entry first**.

## Current position

- **Next session:** W1S1 (continued) — Java 17, uv project, Spark + Delta smoke test, Kafka
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

- Databricks Free Edition constraints (serverless only, streaming only with AvailableNow, no outbound access to local Kafka) — not to be confused with paid trials
- Why Spark runs natively on macOS: Docker runs inside a Linux VM that reserves RAM; native leaves 8–10 GB for the driver

## Parking lot

> Good ideas and tangents that are not for the current session. Reviewed on Flex Days.

- —

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
