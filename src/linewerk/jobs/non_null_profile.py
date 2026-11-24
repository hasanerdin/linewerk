"""Non-null counts per feature, cached as JSON, then fill ratios per station / line.

Step 1 (Spark, only if the cache is missing or --refresh):
    one single-pass aggregation per file -> data/profiling/<stem>_non_null.json
Step 2 (pure Python): station table, line table, total filled cells.

Run:  uv run python -m linewerk.jobs.non_null_profile [--refresh]
"""

import argparse
import json
from pathlib import Path

from linewerk.config import DATA_DIR, RAW_DIR
from linewerk.naming import NON_FEATURE_COLUMNS
from linewerk.quality.fill_ratio import (
    LineFill,
    StationFill,
    line_fill,
    station_fill,
    total_filled_cells,
)

PROFILING_DIR = DATA_DIR / "profiling"
NUMERIC_FILES = ["train_numeric.csv", "test_numeric.csv"]


def cache_path(csv_name: str) -> Path:
    return PROFILING_DIR / f"{Path(csv_name).stem}_non_null.json"


def source_fingerprint(csv_path: Path) -> dict:
    """Size and mtime of the raw file; a re-download or edit changes at least one."""
    stat = csv_path.stat()
    return {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def stale_reason(cached: dict, csv_path: Path) -> str | None:
    """None if the cache still matches the raw file, otherwise why it does not."""
    expected = cached.get("source")
    if expected is None:
        return "no source fingerprint (old cache format)"
    actual = source_fingerprint(csv_path)
    if expected != actual:
        return f"source changed: cached {expected}, now {actual}"
    return None


def compute_non_null(csv_path: Path) -> dict:
    """Single Spark pass over the file. Imports Spark lazily: cached runs never start a JVM."""
    from linewerk.ingest.schema import schema_for_file
    from linewerk.quality.profiling import TOTAL_ROWS_COLUMN, non_null_counts
    from linewerk.spark import get_spark

    # Fingerprint before reading: if the file changes during the scan, the
    # next run sees a mismatch and recomputes instead of trusting mixed data.
    source = source_fingerprint(csv_path)
    spark = get_spark("linewerk-non-null-profile")
    df = spark.read.csv(str(csv_path), header=True, schema=schema_for_file(csv_path))
    features = [c for c in df.columns if c not in NON_FEATURE_COLUMNS]
    row = non_null_counts(df, features).collect()[0].asDict()
    total_rows = row.pop(TOTAL_ROWS_COLUMN)
    return {"file": csv_path.name, "source": source, "total_rows": total_rows, "non_null": row}


def load_or_compute(csv_name: str, refresh: bool) -> dict:
    path = cache_path(csv_name)
    csv_path = RAW_DIR / csv_name
    if path.exists() and not refresh:
        cached = json.loads(path.read_text())
        reason = stale_reason(cached, csv_path)
        if reason is None:
            return cached
        print(f"{path.name} is stale ({reason}), recomputing")

    result = compute_non_null(csv_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=1))
    print(f"wrote {path}")
    return result


def format_station_table(stations: list[StationFill]) -> str:
    lines = [
        "| line | station | n_features | min_non_null | max_non_null | fill_ratio |",
        "|---:|---:|---:|---:|---:|---:|",
    ]
    for s in stations:
        lines.append(
            f"| {s.line} | {s.station} | {s.n_features} | {s.min_non_null:,} "
            f"| {s.max_non_null:,} | {s.fill_ratio:.4f} |"
        )
    return "\n".join(lines)


def format_line_table(lines_: list[LineFill]) -> str:
    lines = ["| line | n_features | fill_ratio |", "|---:|---:|---:|"]
    for line in lines_:
        lines.append(f"| {line.line} | {line.n_features} | {line.fill_ratio:.4f} |")
    return "\n".join(lines)


def report(profile: dict) -> None:
    non_null, total_rows = profile["non_null"], profile["total_rows"]
    stations = station_fill(non_null, total_rows)
    matching = [s for s in stations if s.all_or_nothing]
    breaking = [s for s in stations if not s.all_or_nothing]

    print(f"\n## {profile['file']} ({total_rows:,} rows, {len(non_null)} features)\n")
    print(format_station_table(stations))
    print(f"\nmin == max: {len(matching)} / {len(stations)} stations")
    print("not matching: " + (", ".join(f"L{s.line}_S{s.station}" for s in breaking) or "-"))
    print()
    print(format_line_table(line_fill(non_null, total_rows)))
    print(f"\nTotal filled cells (expected silver_measurements rows): "
          f"{total_filled_cells(non_null):,}")


def main(refresh: bool) -> None:
    for name in NUMERIC_FILES:
        report(load_or_compute(name, refresh))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh", action="store_true", help="recompute with Spark")
    main(refresh=parser.parse_args().refresh)
