"""W1S4 time profiling.

Block 1 (headers only, no Spark): per station, the F / D counts and how many
D columns have an F column at D - OFFSET.
Block 2 (Spark, train and test date files, three passes each):
  1. global min / max over every D column (the time span of the file)
  2. per station: how many parts visit it, in how many rows its D timestamps
     disagree, and the largest in-row spread
  3. for QUANTILE_STATIONS: quantiles of the in-row spread over the rows where
     it is non-zero, also as a share of the time span
Block 2 ends with the span of train and test together.

Run:  uv run python -m linewerk.jobs.profile_time [--sample]
      --sample reads data/sample/<stem>_100k.csv (first 100k rows of each file).
"""

import argparse
import time
from collections.abc import Callable
from pathlib import Path

from linewerk.config import DATA_DIR, RAW_DIR
from linewerk.ingest.schema import read_header
from linewerk.quality.date_mapping import StationNumbers, numbers_by_station

NUMERIC_FILE = "train_numeric.csv"
DATE_FILE = "train_date.csv"
SPREAD_FILES = ["train_date.csv", "test_date.csv"]
SAMPLE_DIR = DATA_DIR / "sample"
OFFSET = 1  # hypothesis from the source description: D = F + 1
# Line 1: the only stations where max_spread was large; is it one outlier part or many?
QUANTILE_STATIONS = [(1, 24), (1, 25)]
QUANTILE_PROBS = [0.5, 0.95, 0.99]


def timed[T](fn: Callable[[], T]) -> tuple[T, float]:
    """Run fn and return (its result, wall-clock seconds)."""
    start = time.perf_counter()
    result = fn()
    return result, time.perf_counter() - start


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


def format_quantile_table(rows: list[dict], probs: list[float], span: float) -> str:
    """Each quantile as 'value (share of span)', so the decision ratio is read directly."""
    labels = [f"p{p * 100:g}" for p in probs]  # 0.5 -> p50, 0.95 -> p95
    lines = [
        f"| line | station | n_disagree | {' | '.join(labels)} |",
        "|---:|---:|---:|" + "---:|" * len(probs),
    ]
    for r in rows:
        if r["quantiles"] is None:  # the station's D columns never disagree
            cells = ["-"] * len(probs)
        else:
            cells = [f"{q:.2f} ({q / span:.2%})" for q in r["quantiles"]]
        lines.append(
            f"| {r['line']} | {r['station']} | {r['n_disagree']:,} | {' | '.join(cells)} |"
        )
    return "\n".join(lines)


def report_time_spread(date_path: Path) -> tuple[float, float]:
    """Three Spark passes over one date file; returns its (min, max) D value."""
    # Lazy on purpose: linewerk.spark pulls in delta-spark, which Block 1 (headers only) does not need.
    from linewerk.ingest.schema import schema_for_file
    from linewerk.quality.date_mapping import (
        global_time_range,
        spread_quantiles,
        station_time_spread,
    )
    from linewerk.spark import get_spark

    spark = get_spark("linewerk-profile-time")
    df = spark.read.csv(str(date_path), header=True, schema=schema_for_file(date_path))

    # The span comes first: the quantile table below is expressed as a share of it.
    (lo, hi), t_range = timed(lambda: global_time_range(df))
    span = hi - lo
    rows, t_spread = timed(
        lambda: [r.asDict() for r in station_time_spread(df).collect()]
    )
    q_rows, t_quant = timed(
        lambda: [
            r.asDict()
            for r in spread_quantiles(df, QUANTILE_STATIONS, QUANTILE_PROBS).collect()
        ]
    )

    print(f"\n## {date_path.name}\n")
    print(f"time span over all D columns: {lo:.2f} .. {hi:.2f} (span {span:.2f})")

    print("\n### In-row D spread per station\n")
    print(format_spread_table(rows))
    disagree = [r for r in rows if r["rows_hi_ne_lo"] > 0]
    print(
        f"\nstations where D columns disagree in some row: {len(disagree)} / {len(rows)}"
    )

    print("\n### Spread quantiles over rows where hi != lo, as value (share of span)\n")
    print(format_quantile_table(q_rows, QUANTILE_PROBS, span))

    print(
        f"\nseconds: range {t_range:.1f}, spread {t_spread:.1f}, quantiles {t_quant:.1f}"
    )
    return lo, hi


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
    ranges = [report_time_spread(spread_path(name, sample)) for name in SPREAD_FILES]

    # Train and test may sit on one shared time axis; if so, their spans overlap.
    lo = min(r[0] for r in ranges)
    hi = max(r[1] for r in ranges)
    print(f"\n## {' + '.join(SPREAD_FILES)}\n")
    for name, (f_lo, f_hi) in zip(SPREAD_FILES, ranges, strict=True):
        print(f"{name}: {f_lo:.2f} .. {f_hi:.2f}")
    print(f"together: {lo:.2f} .. {hi:.2f} (span {hi - lo:.2f})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", action="store_true", help="use the 100k-row sample")
    main(sample=parser.parse_args().sample)
