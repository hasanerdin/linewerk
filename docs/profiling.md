# Data profiling

> Bosch Production Line Performance, in-scope files only (numeric + date, train + test).
> W1S3: shape and sparsity. W1S4 adds time and labels.

## Files

| file | size on disk (GiB) | rows | columns |
|---|---:|---:|---:|
| train_numeric.csv | 1.99 | 1,183,747 | 970 |
| train_date.csv | 2.69 | 1,183,747 | 1,157 |
| test_numeric.csv | 1.99 | 1,183,748 | 969 |
| test_date.csv | 2.69 | 1,183,748 | 1,157 |

- numeric = `Id` + 968 features (+ `Response` in train only); date = `Id` + 1,156 timestamps.
- Train and test differ by one row.

Source: `uv run python -m linewerk.jobs.profile_shape`

## Column structure

- **4 lines** (L0–L3), **52 station IDs** (S0–S51). Only 50 of them have numeric features.
- **Station numbers are global:** no number appears on two lines, so `station` alone is a key.
  Lines: L0 = S0–S23 (24), L1 = S24–S25 (2), L2 = S26–S28 (3), L3 = S29–S51 (23).
- **S42 and S46 appear only in the date files** (8 and 1 `D` columns, 0 `F`).
- **D > F:** 1,156 `D` vs 968 `F` columns. No station has more F than D: 27 stations have equal counts, 25 have more D (e.g. S24: 229 F / 288 D, S25: 284 F / 333 D).

## Sparsity

Date-file sparsity is profiled in W1S4.

This section uses train_numeric only. test_numeric is nearly the same: line fills differ by at most 0.0002 and station fills by at most 0.001. Test also has 35/50 all-or-nothing stations (see below), with S48 in place of S36 in the list of stations that break the pattern.

### Per line

| line | stations (numeric) | n_features | fill_ratio | filled_cells | share of filled cells |
|---:|---:|---:|---:|---:|---:|
| 0 | 24 | 168 | 0.2007 | 39,903,521 | 18.4% |
| 1 | 2 | 513 | 0.0215 | 13,070,268 | 6.0% |
| 2 | 3 | 42 | 0.1006 | 5,002,522 | 2.3% |
| 3 | 21 | 245 | 0.5474 | 158,765,619 | 73.3% |

Column count is a poor proxy for volume: L1 has 53% of the features but only ~6% of the filled cells, while L3 holds ~73%. Skew in W2 will come from L3, not L1.

### Per station

`min_non_null` / `max_non_null` = smallest and largest non-null count among the station's features.

| line | station | n_features | min_non_null | max_non_null | fill_ratio |
|---:|---:|---:|---:|---:|---:|
| 0 | 0 | 12 | 673,862 | 673,862 | 0.5693 |
| 0 | 1 | 2 | 673,902 | 673,904 | 0.5693 |
| 0 | 2 | 9 | 339,774 | 339,774 | 0.2870 |
| 0 | 3 | 9 | 334,708 | 334,708 | 0.2828 |
| 0 | 4 | 2 | 335,243 | 335,295 | 0.2832 |
| 0 | 5 | 2 | 339,512 | 339,512 | 0.2868 |
| 0 | 6 | 3 | 338,988 | 338,988 | 0.2864 |
| 0 | 7 | 3 | 335,698 | 335,698 | 0.2836 |
| 0 | 8 | 3 | 673,881 | 673,881 | 0.5693 |
| 0 | 9 | 12 | 225,659 | 225,678 | 0.1906 |
| 0 | 10 | 12 | 224,523 | 224,540 | 0.1897 |
| 0 | 11 | 12 | 225,452 | 225,452 | 0.1905 |
| 0 | 12 | 12 | 242,061 | 242,061 | 0.2045 |
| 0 | 13 | 2 | 242,065 | 242,065 | 0.2045 |
| 0 | 14 | 9 | 120,625 | 120,625 | 0.1019 |
| 0 | 15 | 9 | 121,445 | 121,445 | 0.1026 |
| 0 | 16 | 2 | 119,139 | 119,139 | 0.1006 |
| 0 | 17 | 2 | 123,027 | 123,027 | 0.1039 |
| 0 | 18 | 3 | 121,081 | 121,081 | 0.1023 |
| 0 | 19 | 3 | 121,027 | 121,027 | 0.1022 |
| 0 | 20 | 3 | 242,111 | 242,111 | 0.2045 |
| 0 | 21 | 14 | 81,368 | 81,409 | 0.0687 |
| 0 | 22 | 14 | 80,591 | 80,599 | 0.0681 |
| 0 | 23 | 14 | 80,290 | 80,290 | 0.0678 |
| 1 | 24 | 229 | 11,834 | 132,922 | 0.0341 |
| 1 | 25 | 284 | 1,243 | 55,466 | 0.0114 |
| 2 | 26 | 14 | 227,011 | 227,011 | 0.1918 |
| 2 | 27 | 14 | 120,729 | 120,729 | 0.1020 |
| 2 | 28 | 14 | 9,583 | 9,583 | 0.0081 |
| 3 | 29 | 53 | 1,069,324 | 1,119,629 | 0.9434 |
| 3 | 30 | 68 | 810,256 | 1,119,811 | 0.8229 |
| 3 | 31 | 4 | 39,003 | 39,003 | 0.0329 |
| 3 | 32 | 1 | 24,543 | 24,543 | 0.0207 |
| 3 | 33 | 10 | 1,114,695 | 1,114,695 | 0.9417 |
| 3 | 34 | 4 | 1,115,118 | 1,115,118 | 0.9420 |
| 3 | 35 | 8 | 552,047 | 552,108 | 0.4664 |
| 3 | 36 | 8 | 569,031 | 569,032 | 0.4807 |
| 3 | 37 | 4 | 1,120,394 | 1,120,394 | 0.9465 |
| 3 | 38 | 3 | 27,142 | 27,142 | 0.0229 |
| 3 | 39 | 4 | 59,908 | 59,908 | 0.0506 |
| 3 | 40 | 8 | 59,896 | 59,914 | 0.0506 |
| 3 | 41 | 14 | 59,913 | 59,913 | 0.0506 |
| 3 | 43 | 8 | 30,545 | 30,551 | 0.0258 |
| 3 | 44 | 8 | 29,804 | 29,804 | 0.0252 |
| 3 | 45 | 5 | 59,932 | 59,932 | 0.0506 |
| 3 | 47 | 11 | 59,955 | 59,955 | 0.0506 |
| 3 | 48 | 6 | 59,923 | 59,923 | 0.0506 |
| 3 | 49 | 7 | 29,663 | 29,673 | 0.0251 |
| 3 | 50 | 7 | 30,359 | 30,359 | 0.0256 |
| 3 | 51 | 4 | 59,853 | 59,853 | 0.0506 |

Source: `uv run python -m linewerk.jobs.non_null_profile`

## Expected silver size

| split | non-null cells = `silver_measurements` rows | fill (of rows × 968) |
|---|---:|---:|
| train | 216,741,930 | 18.9% |
| test | 216,694,404 | 18.9% |
| **total** | **433,436,334** | |

About 81% of the wide cells are empty. Dropping nulls during the unpivot (W2S2) is what keeps silver at ~433M rows. Without it, silver would have ~2.29B rows.

## Hypotheses to verify

These are observations from counts, not checked on real rows yet.

| # | Observation | What it suggests | Verify in |
|---|---|---|---|
| H1 | In 35 of 50 stations, every feature has the same non-null count (min = max). | A part is measured on all of a station's features or on none ("all-or-nothing"). Equal counts are necessary for this but not sufficient: they could come from different rows. | W2S2, after the unpivot: group by (part, station) and check that every part has either all of the station's features or none |
| H2 | For 11 of the 15 other stations the spread is tiny (1–61). For S24, S25, S29 and S30 it is large (S24: 11,834–132,922; S25: 1,243–55,466). | Different product variants may run different measurement programs at the same station, and the small spreads may be isolated missing values. | W2S4 (flow paths), check whether the feature sets group by variant |
| H3 | Fills add up: S2 + S3 ≈ S0 ≈ S8 (0.287 + 0.283 ≈ 0.569). S9 + S10 + S11 ≈ 0.57. S14 + S15 ≈ S12 ≈ S20 (≈ 0.204). Several L3 stations sit at exactly 0.0506, and S43/S44/S49/S50 sit at ≈ 0.025 each. | Some stations may be parallel copies that a part chooses between, not steps it goes through one after another. | W2S4 (flow paths), check whether S2 and S3 (and the others) rule each other out per part |
| H4 | Every station has at least as many D columns as F columns. S42 and S46 have only D columns. | Each F may have a matching D timestamp, plus extra D columns for events with no measured value. The source description suggests an offset in the numbering. | W1S4, task 1, on real rows |

## How measured

- **Machine:** Apple M3 (8 cores: 4P + 4E), 16 GB RAM, internal SSD, macOS native (no Docker). Spark 4.1.1 `local[*]`, `spark.driver.memory=8g`.
- **Single pass:** one `df.agg(count(lit(1)), count(c) for each of 968 features)` = one scan of the file (`quality/profiling.py`). It takes **~15 s** on train_numeric.
- **Naive loop:** `filter(isNull).count()` per column means one Spark job and one full scan per feature. Measured on 20 features and extrapolated to 968, that is **~24 min** (`jobs/null_count_benchmark.py`).
- **Cache:** the results are saved to `data/profiling/<file>_non_null.json`, together with the raw file's size and mtime. All tables above come from this cache in plain Python, with no Spark. The cache is recomputed only with `--refresh` or when the raw file changes.
