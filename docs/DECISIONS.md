# Linewerk — Decision Log

> Lightweight architecture decision records. One entry per meaningful decision: what, why, and what it costs.
> New entries go at the bottom. A superseded decision is not deleted; it gets the status `superseded by DEC-xxx`.

## Index

| ID | Title | Status |
|---|---|---|
| DEC-001 | Dataset: Bosch Production Line Performance | accepted |
| DEC-002 | Train = historical production, test = live production | accepted |
| DEC-003 | Kafka runs locally in Docker, not in a managed service | accepted |
| DEC-004 | Streaming runs locally; Databricks receives data via Volume upload | accepted |
| DEC-005 | Control limits as an SCD Type 2 dimension | accepted |
| DEC-006 | Databricks Asset Bundles instead of Terraform | accepted |
| DEC-007 | PySpark runs natively on macOS, not in Docker | accepted |
| DEC-008 | Pinned runtime: Spark 4.1.1, Delta 4.3.1, Python 3.12, Java 17 | accepted |
| DEC-009 | Kafka container without a persistent volume | accepted |
| DEC-010 | Raw files are stored unzipped as plain CSV | accepted |

---

### DEC-001 — Dataset: Bosch Production Line Performance (2026-11-19, W0)
- **Context:** The project needs a real dataset that is large enough to require Spark and that fits a credible story.
- **Options:** Bosch production line data (Kaggle) / German fuel prices (Tankerkönig) / Wikimedia live stream / NYC Taxi
- **Decision:** Bosch Production Line Performance, `numeric` and `date` files for train and test. `categorical` is a stretch goal.
- **Why:** Real manufacturing data from 4 lines and 51 stations, which matches my industrial inspection background. It is wide and sparse, so it creates real data engineering problems (unpivot, layout, skew). NYC Taxi is overused; Wikimedia is not industrial; Tankerkönig has stricter licence terms.
- **Consequences:** Kaggle competition rules apply: raw data is never committed and the repo ships a download script only. Features are anonymised, so there are no engineering tolerances (see DEC-005).

### DEC-002 — Train = historical production, test = live production (2026-11-19, W0)
- **Context:** Streaming must serve the story, not be added for its own sake.
- **Options:** Simulate synthetic sensor data / replay the train set / replay the test set
- **Decision:** The labelled train set is treated as historical production (baselines, control limits, findings). The unlabelled test set is replayed through Kafka in timestamp order as live production.
- **Why:** This mirrors a real setup: limits are learned from history and applied to new production. It also uses real data for the stream instead of synthetic data.
- **Consequences:** Live alerts cannot be validated against labels. Their usefulness is argued through the lift measured on the train set.

### DEC-003 — Kafka runs locally in Docker, not in a managed service (2026-11-19, W0)
- **Context:** The project budget is €0. Managed Kafka trials are credit-based, time-limited and require a payment method.
- **Options:** Confluent Cloud trial / local Kafka in Docker (KRaft)
- **Decision:** Single-node Kafka in Docker using KRaft mode.
- **Why:** No cost and no billing risk after a trial ends. Running and configuring Kafka myself is also more instructive.
- **Consequences:** No managed features (Schema Registry UI, connectors). Single node, so replication and broker failure are discussed rather than demonstrated.

### DEC-004 — Streaming runs locally; Databricks receives data via Volume upload (2026-11-19, W0)
- **Context:** Databricks Free Edition is serverless-only, supports Structured Streaming only with `Trigger.AvailableNow()`, and restricts outbound internet access, so it cannot read from local Kafka.
- **Options:** Paid Databricks trial / run everything locally / hybrid: local streaming + Databricks for the lakehouse and serving
- **Decision:** Hybrid. Continuous streaming (Kafka → Structured Streaming) runs locally. Data is uploaded to a Unity Catalog Volume with the Databricks SDK and ingested with Auto Loader (`AvailableNow`). Gold, jobs and the dashboard run on Databricks.
- **Why:** Shows real continuous streaming and real Databricks features at €0. Upload + Auto Loader is a common production pattern.
- **Consequences:** Two runtimes. This is mitigated by one shared transformation library (pure DataFrame functions) used everywhere, which is a deliberate design point.

### DEC-005 — Control limits as an SCD Type 2 dimension (2026-11-19, W0)
- **Context:** Measurements are anonymised, so control limits must be derived statistically (mean ± 3σ). In real SPC, limits are re-baselined when the process shifts.
- **Options:** Static limits computed once / recompute limits every period and overwrite / versioned limits (SCD2)
- **Decision:** `dim_control_limit` as SCD Type 2 (`valid_from`, `valid_to`, `is_current`), maintained with Delta `MERGE`. A new version starts only when the re-baselining rule fires (rule defined in W3S2). Facts join to the version valid at the measurement timestamp.
- **Why:** Judging a historical measurement against today's limit produces false alarms. Point-in-time correctness requires history.
- **Consequences:** More complex joins (as-of join). The naive vs point-in-time alarm difference will be measured in W3S3.

### DEC-006 — Databricks Asset Bundles instead of Terraform (2026-11-19, W0)
- **Context:** Databricks jobs should be defined as code. Free Edition has no account console or account-level APIs.
- **Options:** Terraform Databricks provider / Asset Bundles / manual UI setup
- **Decision:** Databricks Asset Bundles.
- **Why:** Bundles are Databricks' native deployment tool for jobs and code, work at workspace level and fit Free Edition. Terraform is already demonstrated in the Zephyrwerk project.
- **Consequences:** No IaC for account-level resources, which are not available in Free Edition anyway.

### DEC-007 — PySpark runs natively on macOS, not in Docker (2026-11-19, W0)
- **Context:** Development machine is a MacBook M3 with 16 GB RAM. Docker on macOS runs in a VM that reserves memory.
- **Options:** Spark in Docker / native PySpark with Java 17
- **Decision:** Native PySpark with Java 17, managed with `uv`. Only Kafka runs in Docker (~1–2 GB).
- **Why:** Leaves 8–10 GB for the Spark driver, which the wide-to-long transformation needs.
- **Consequences:** The local setup is less portable. This is mitigated by pinned versions and clear setup steps in the README; CI runs on Linux with Java installed in the runner.

### DEC-008 — Pinned runtime: Spark 4.1.1, Delta 4.3.1, Python 3.12, Java 17 (2026-11-21, W1S1)
- **Context:** delta-spark hooks into Spark internals, so Spark and Delta versions must match exactly; the same gold code must later run on Databricks.
- **Options:** Latest PySpark (4.1.4) / Spark 4.0.x with Delta 4.0 / Spark 4.1.1 with Delta 4.3.1
- **Decision:** `pyspark==4.1.1`, `delta-spark==4.3.1`, Python 3.12, Temurin JDK 17; exact pins plus a committed `uv.lock`.
- **Why:** delta-spark 4.3.1 declares `pyspark>=4.0.1,<=4.1.1`, so 4.1.4 was rejected by the resolver. Spark 4.1, Python 3.12 and JDK 17 match Databricks Runtime 18 / serverless environment 5, which keeps local and Databricks behaviour close.
- **Consequences:** Not on the newest PySpark patch. Revisit when a Delta release supports a newer Spark, or when the Databricks serverless runtime moves on (W5).

### DEC-009 — Kafka container without a persistent volume (2026-11-22, W1S1)
- **Context:** The local Kafka broker stores topic data inside the container. A volume would keep it across `docker compose down`.
- **Options:** Named volume for the Kafka log dirs / no volume, data lives in the container
- **Decision:** No volume. Topics and data survive `stop`/`start` but are lost on `down`.
- **Why:** The stream is a replay of the test set (DEC-002); topics are recreated by script and the data can always be produced again, so persistence adds nothing. It also avoids volume permission issues with the `apache/kafka` image (non-root user).
- **Consequences:** `docker compose down` resets Kafka; streaming checkpoints must be reset at the same time or the consumer will point at offsets that no longer exist. Revisit if a long-running demo needs history.

### DEC-010 — Raw files are stored unzipped as plain CSV (2026-11-22, W1S2)
- **Context:** Kaggle delivers `.csv.zip` files. Spark has no codec for zip archives and would read them as garbage without an error.
- **Options:** Keep zip and unzip on the fly / re-compress as `.csv.gz` / unzip to plain CSV
- **Decision:** `ingest/download.py` unzips to plain CSV in `data/raw/` and deletes the zip.
- **Why:** Plain CSV is splittable, so Spark reads one file with many parallel tasks; gzip is not splittable (one task per file). Disk cost is acceptable: 9.36 GiB unzipped vs 658 MiB zipped, ~80 GB free.
- **Consequences:** ~8.7 GiB more disk use. Raw CSV is only read once into bronze (Delta/Parquet, compressed), after which the CSVs could be deleted if disk gets tight.