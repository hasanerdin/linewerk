"""W1S4 time profiling.

Block 1 (headers only, no Spark): per station, the F / D counts and how many
D columns have an F column at D - OFFSET.
Block 2 (Spark, one pass per date file, train and test): per station, how many
parts visit it, in how many rows its D timestamps disagree, and the largest
in-row spread.

Run:  uv run python -m linewerk.jobs.profile_time [--sample]
      --sample reads data/sample/<stem>_100k.csv (first 100k rows of each file).
"""

import argparse
import time
from pathlib import Path

from linewerk.config import DATA_DIR, RAW_DIR
from linewerk.ingest.schema import read_header
from linewerk.quality.date_mapping import StationNumbers, numbers_by_station

NUMERIC_FILE = "train_numeric.csv"
DATE_FILE = "train_date.csv"
SPREAD_FILES = ["train_date.csv", "test_date.csv"]
SAMPLE_DIR = DATA_DIR / "sample"
OFFSET = 1  # hypothesis from the source description: D = F + 1


def format_mapping_table(stations: list[StationNumbers], offset: int) -> str:
    lines = [
        f"| line | station | n_F | n_D | D without F at D-{offset} |",
        "|---:|---:|---:|---:|---|",
    ]
    for s in stations:
        unmatched = s.unmatched_d(offset)
        shown = ", ".join(f"D{d}" for d in unmatched) or "-"
        lines.append(
            f"| {s.line} | {s.station} | {len(s.f_numbers)} | {len(s.d_numbers)} | {shown} |"
        )
    return "\n".join(lines)


def format_spread_table(rows: list[dict]) -> str:
    lines = [
        "| line | station | n_D | visited_rows | rows_hi_ne_lo | max_spread |",
        "|---:|---:|---:|---:|---:|---:|",
    ]
    for r in rows:
        spread = "-" if r["max_spread"] is None else f"{r['max_spread']:.2f}"
        lines.append(
            f"| {r['line']} | {r['station']} | {r['n_d_columns']} | {r['visited_rows']:,} "
            f"| {r['rows_hi_ne_lo']:,} | {spread} |"
        )
    return "\n".join(lines)


def report_time_spread(date_path: Path) -> None:
    """Single Spark pass over one date file."""
    # Lazy on purpose: linewerk.spark pulls in delta-spark, which Block 1 (headers only) does not need.
    from linewerk.ingest.schema import schema_for_file
    from linewerk.quality.date_mapping import station_time_spread
    from linewerk.spark import get_spark

    spark = get_spark("linewerk-profile-time")
    df = spark.read.csv(str(date_path), header=True, schema=schema_for_file(date_path))
    start = time.perf_counter()
    rows = [r.asDict() for r in station_time_spread(df).collect()]
    elapsed = time.perf_counter() - start

    print(f"\n## In-row D spread per station: {date_path.name}\n")
    print(format_spread_table(rows))
    disagree = [r for r in rows if r["rows_hi_ne_lo"] > 0]
    print(
        f"\nstations where D columns disagree in some row: {len(disagree)} / {len(rows)}"
    )
    print(f"aggregation + collect: {elapsed:.1f} s")


def report_mapping() -> None:
    columns = read_header(RAW_DIR / NUMERIC_FILE) + read_header(RAW_DIR / DATE_FILE)
    stations = numbers_by_station(columns)

    print(format_mapping_table(stations, OFFSET))
    n_d = sum(len(s.d_numbers) for s in stations)
    n_unmatched = sum(len(s.unmatched_d(OFFSET)) for s in stations)
    clean = sum(1 for s in stations if not s.unmatched_d(OFFSET))
    print(f"\nD columns: {n_d}, without F at D-{OFFSET}: {n_unmatched}")
    print(f"stations where every D has its F: {clean} / {len(stations)}")


def spread_path(csv_name: str, sample: bool) -> Path:
    return (
        SAMPLE_DIR / f"{Path(csv_name).stem}_100k.csv" if sample else RAW_DIR / csv_name
    )


def main(sample: bool) -> None:
    report_mapping()
    for name in SPREAD_FILES:
        report_time_spread(spread_path(name, sample))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", action="store_true", help="use the 100k-row sample")
    main(sample=parser.parse_args().sample)
