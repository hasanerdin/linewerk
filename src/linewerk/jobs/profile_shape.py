from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession

from linewerk.config import RAW_DIR
from linewerk.ingest.schema import read_header, schema_for_file
from linewerk.transforms.parsing import ColumnInfo, build_column_lookup

SHAPE_FILES = [
    "train_numeric.csv",
    "train_date.csv",
    "test_numeric.csv",
    "test_date.csv",
]
BYTES_PER_GIB = 1024**3


@dataclass(frozen=True)
class FileShape:
    file: str
    size_gib: float
    rows: int
    columns: int


def file_size_gib(path: Path) -> float:
    return path.stat().st_size / BYTES_PER_GIB


def column_count(path: Path) -> int:
    """Column count from the header line only, no Spark needed."""
    return len(read_header(path))


def row_count(spark: SparkSession, path: Path) -> int:
    """Data rows (header excluded), read with the header-derived explicit schema."""
    return spark.read.csv(str(path), header=True, schema=schema_for_file(path)).count()


def file_shape(spark: SparkSession, path: Path) -> FileShape:
    return FileShape(
        file=path.name,
        size_gib=file_size_gib(path),
        rows=row_count(spark, path),
        columns=column_count(path),
    )


def format_shape_table(shapes: list[FileShape]) -> str:
    """Markdown table, ready to paste into docs/profiling.md."""
    lines = [
        "| file | size on disk (GiB) | rows | columns |",
        "|---|---:|---:|---:|",
    ]
    for s in shapes:
        lines.append(f"| {s.file} | {s.size_gib:.2f} | {s.rows:,} | {s.columns:,} |")
    return "\n".join(lines)


def print_shape(lookup: list[tuple[str, ColumnInfo]]) -> None:
    hierarchy = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    for _, info in lookup:
        hierarchy[info.line][info.station][info.kind].add(info.number)

    print(f"Total distinct lines: {len(hierarchy)}\n")

    for line, stations in sorted(hierarchy.items()):
        print(f"Line {line} -> {len(stations)} distinct station(s):")
        for station, kinds in sorted(stations.items()):
            f_count = len(kinds["F"])
            d_count = len(kinds["D"])
            print(
                f"  └── Station {station}: {f_count} F column(s), {d_count} D column(s)"
            )


def profile_shape(file_path: Path) -> list[tuple[str, ColumnInfo]]:
    lookup = build_column_lookup(read_header(file_path))
    print_shape(lookup)
    return lookup


def main() -> None:
    from linewerk.spark import get_spark

    spark = get_spark("linewerk-profile-shape")
    shapes = [file_shape(spark, RAW_DIR / name) for name in SHAPE_FILES]
    print(format_shape_table(shapes))


if __name__ == "__main__":
    main()
