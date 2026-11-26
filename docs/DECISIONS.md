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
| DEC-011 | Explicit schema generated from the CSV header | accepted |
| DEC-012 | Feature branches with squash-merged PRs | accepted |
| DEC-013| Parse feature names once from the header into a lookup table | accepted |
| DEC-014 | `station` is the natural key of `dim_station`; `line` is an attribute | accepted |
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

### DEC-011 — Explicit schema generated from the CSV header (2026-11-23, W1S2)
- **Context:** The raw files have 970 (numeric) and 1,157 (date) columns. Spark can infer types or receive an explicit schema.
- **Options:** `inferSchema=True` / `inferSchema` with `samplingRatio` / explicit `StructType` written by hand / explicit `StructType` generated from the header
- **Decision:** `ingest/schema.py` reads only the header line in plain Python and builds the schema: `Id` int (non-nullable), `Response` int, every `L<n>_S<n>_F<n>` / `L<n>_S<n>_D<n>` column double. Any other column name raises a `ValueError`.
- **Why:** inferSchema costs a full extra pass (train_numeric read + count: 5.64 s vs 1.77 s explicit) and its types are unstable: 19 columns came out as int on a 100k-row sample, 16 on the full file. Generating from the header keeps the schema in sync with column order without writing 2,000+ fields by hand.
- **Consequences:** All features are double even when values look like integers. A renamed or new column stops the pipeline until a human decides (fail-fast on schema drift). For streaming (W4) the schema is fixed, so header-vs-schema order must be checked (`enforceSchema=False`).

### DEC-012 — Feature branches with squash-merged PRs (2026-11-23, W1S2)
- **Context:** Until W1S1 all commits went straight to `main`. The repo is also a portfolio, and CI arrives in W5.
- **Options:** Commit to `main` / Gitflow with `develop` / one feature branch per session or feature, squash-merged via PR
- **Decision:** One branch per session or feature, a PR with a short summary and the measured numbers, squash merge, delete the branch. Then press *Sync now* (the Claude project syncs `main` only).
- **Why:** Small, reviewable units and a readable history that shows how I work. Gitflow is overhead for a single developer.
- **Consequences:** Code on a branch is not visible to the mentor via sync until merged — paste it for review. Branch protection with required CI checks follows in W5.

### DEC-013 — Parse feature names once from the header into a lookup table (2026-11-24, W1S3)
- **Context:** Line, station and feature are encoded in the column names (`L3_S50_F4243`). They are needed for every silver row and for the station/line dimensions.
- **Options:** A: parse the name per row in Spark after the unpivot / B: parse the ~2,100 header names once in plain Python and join the small lookup table.
- **Decision:** B. One regex with capture groups in `naming.py` is the single source of the naming rule; `parse_column` fails fast on unknown names.
- **Why:** ~2,100 names vs ~433M silver rows; string parsing per row would repeat the same work millions of times. One regex keeps schema validation and parsing consistent.
- **Consequences:** The lookup must be built from the same header as the data; a new column naming scheme fails loudly at parse time instead of producing null line/station values.

### DEC-014 — `station` is the natural key of `dim_station`; `line` is an attribute (2026-11-24, W1S3)
- **Context:** Station numbers could have been local to each line (S0 on every line) or global.
- **Options:** A: composite key (line, station) / B: station alone, with line as an attribute.
- **Decision:** B.
- **Why:** Profiling of all four headers shows station numbers S0–S51 are unique across lines (L0 = S0–S23, L1 = S24–S25, L2 = S26–S28, L3 = S29–S51); no number appears on two lines.
- **Consequences:** Simpler joins and keys. If a future data source reuses station numbers across lines, the key must become composite. Add a uniqueness check (station → exactly one line) in W2S4.

### DEC-015 — One timestamp per part × station, not per feature (2026-11-26, W1S4)
- **Context:** Silver needs a timestamp for every measurement (replay order, SPC time buckets, flow path). The header rule `D = F + 1` does not hold: 790 of 1,156 D columns have no F at D-1, and many stations have more D than F columns.
- **Options:** A: map each F to its D column per feature (null-pattern check, per-station offsets) / B: one timestamp per part × station = min of the station's D columns / C: B for most stations, A only for Line 1 (S24, S25).
- **Decision:** B — `ts = least(D columns of the station)` per part × station, for all stations.
- **Why:** In 50 of 52 stations all D values of a part are identical (train and test). In L1 S24 the spread is negligible (p99 0.14 units ≈ 0.01% of the 1718-unit span); S25 has a tail (p95 13.4, p99 19.1 ≈ 1.1% of span). For limit-version lookup and ordering, station entry time is precise enough, and B keeps one source of truth for time (`silver_station_visits`).
- **Consequences:** Per-feature timing inside L1 S25 is lost (documented as a caveat). Revisit if an L1 S25 finding depends on order within the station. Add a quality check that D values stay equal within a station.
