"""Null counting on train_numeric: one action per column vs one single pass.

- loop:        filter(isNull).count() for the first LOOP_FEATURES features only
               (all 968 would take ~half an hour).
- single pass: non_null_counts() over all 968 features at once.

Job counts come from the job group of each phase (same numbers as the Jobs tab
in the Spark UI, where each phase also shows up under its own description).

Run:  uv run python -m linewerk.jobs.null_count_benchmark [--no-wait]
"""

import argparse
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from linewerk.config import RAW_DIR
from linewerk.ingest.schema import schema_for_file
from linewerk.naming import NON_FEATURE_COLUMNS
from linewerk.quality.profiling import TOTAL_ROWS_COLUMN, non_null_counts
from linewerk.spark import get_spark

LOOP_FEATURES = 20


def run_phase(spark: SparkSession, group: str, action) -> tuple[float, int, object]:
    """Run `action` under its own job group; return (seconds, job count, result)."""
    sc = spark.sparkContext
    sc.setJobGroup(group, group)
    start = time.perf_counter()
    result = action()
    seconds = time.perf_counter() - start
    jobs = len(sc.statusTracker().getJobIdsForGroup(group))
    sc.setJobGroup("", "")
    return seconds, jobs, result


def null_counts_loop(df: DataFrame, columns: list[str]) -> dict[str, int]:
    return {c: df.filter(F.col(c).isNull()).count() for c in columns}


def null_counts_single_pass(df: DataFrame, columns: list[str]) -> dict[str, int]:
    row = non_null_counts(df, columns).collect()[0].asDict()
    total = row.pop(TOTAL_ROWS_COLUMN)
    return {c: total - n for c, n in row.items()}


def main(wait_for_ui: bool) -> None:
    path = RAW_DIR / "train_numeric.csv"
    spark = get_spark("linewerk-null-count-benchmark")

    # JVM, Catalyst and executor warm-up, outside the measured groups.
    spark.range(1000).selectExpr("sum(id)").collect()

    df = spark.read.csv(str(path), header=True, schema=schema_for_file(path))
    features = [c for c in df.columns if c not in NON_FEATURE_COLUMNS]
    loop_features = features[:LOOP_FEATURES]

    loop_s, loop_jobs, loop_nulls = run_phase(
        spark,
        f"loop: {LOOP_FEATURES} x filter(isNull).count()",
        lambda: null_counts_loop(df, loop_features),
    )
    single_s, single_jobs, single_nulls = run_phase(
        spark,
        f"single pass: {len(features)} features",
        lambda: null_counts_single_pass(df, features),
    )

    # Same answer for the overlapping columns, otherwise the timing means nothing.
    mismatches = {c for c in loop_features if loop_nulls[c] != single_nulls[c]}
    assert not mismatches, f"loop and single pass disagree on {sorted(mismatches)}"

    per_col = loop_s / LOOP_FEATURES
    print()
    print("| approach | features | time (s) | Spark jobs |")
    print("|---|---:|---:|---:|")
    print(
        f"| loop: filter(isNull).count() | {LOOP_FEATURES} | {loop_s:.1f} | {loop_jobs} |"
    )
    print(
        f"| single pass: one agg | {len(features)} | {single_s:.1f} | {single_jobs} |"
    )
    print()
    print(
        f"Loop: {per_col:.2f} s/feature -> ~{per_col * len(features) / 60:.0f} min "
        f"and ~{loop_jobs / LOOP_FEATURES * len(features):.0f} jobs for all {len(features)}"
    )
    print(
        f"Null counts of the first 3 features: "
        f"{ {c: single_nulls[c] for c in loop_features[:3]} }"
    )

    if wait_for_ui:
        input("Spark UI: http://localhost:4040 - Enter to exit")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--no-wait", action="store_true", help="exit without waiting for the UI"
    )
    main(wait_for_ui=not parser.parse_args().no_wait)
