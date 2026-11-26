"""Date columns per station: header-level D <-> F mapping and in-row time spread.

numbers_by_station is pure Python (input: column names, output: plain data), so
it is cheap to test with hand-made headers. station_time_spread works on a Spark
DataFrame of the date file.
"""

from collections import Counter, defaultdict
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from linewerk.transforms.parsing import build_column_lookup


@dataclass(frozen=True)
class StationNumbers:
    line: int
    station: int
    f_numbers: tuple[int, ...]
    d_numbers: tuple[int, ...]

    def unmatched_d(self, offset: int) -> tuple[int, ...]:
        """D numbers with no F at (D - offset) in the same station.

        Header-level test of the "D = F + offset" hypothesis; says nothing
        yet about whether the values are actually filled together.
        """
        f = set(self.f_numbers)
        return tuple(d for d in self.d_numbers if d - offset not in f)


def numbers_by_station(columns: list[str]) -> list[StationNumbers]:
    """F and D numbers per (line, station), sorted by line, station, number.

    Accepts any mix of headers (numeric file, date file, or both concatenated).
    Id / Response are skipped and may appear more than once (concatenating the
    numeric and date headers gives two Id columns). Fails fast with ValueError
    on an unknown column name or on a feature column that appears twice.
    """
    lookup = build_column_lookup(columns)
    duplicates = sorted(
        name for name, n in Counter(name for name, _ in lookup).items() if n > 1
    )
    if duplicates:
        raise ValueError(f"Duplicate feature columns: {', '.join(duplicates)}")

    groups: dict[tuple[int, int], dict[str, set[int]]] = defaultdict(
        lambda: {"F": set(), "D": set()}
    )
    for _, info in lookup:
        groups[(info.line, info.station)][info.kind].add(info.number)

    return [
        StationNumbers(
            line=line,
            station=station,
            f_numbers=tuple(sorted(kinds["F"])),
            d_numbers=tuple(sorted(kinds["D"])),
        )
        for (line, station), kinds in sorted(groups.items())
    ]


def _row_min_max(columns: list[str]) -> tuple[Column, Column]:
    """Smallest / largest non-null value across the columns, within one row.

    least / greatest skip nulls and return null only if every input is null.
    They need at least two arguments, so a one-column station uses the column
    itself for both.
    """
    if len(columns) == 1:
        col = F.col(columns[0])
        return col, col
    cols = [F.col(c) for c in columns]
    return F.least(*cols), F.greatest(*cols)


def _d_columns_by_station(columns: list[str]) -> dict[tuple[int, int], list[str]]:
    """(line, station) -> D column names. Raises ValueError if there are none."""
    d_columns: dict[tuple[int, int], list[str]] = defaultdict(list)
    for name, info in build_column_lookup(columns):
        if info.kind == "D":
            d_columns[(info.line, info.station)].append(name)
    if not d_columns:
        raise ValueError("No D columns among the given columns")

    return dict(d_columns)


def station_time_spread(df: DataFrame) -> DataFrame:
    """One row per station: how far apart its D timestamps are within a part.

    For every row and station, lo / hi are the min / max of the station's D
    columns. Then, in a single aggregation over the whole DataFrame:
      visited_rows   rows where lo is not null (the part has a time at the station)
      rows_hi_ne_lo  rows where hi != lo (the station's D columns disagree)
      max_spread     max(hi - lo); null if the station is never visited

    Output columns: line, station, n_d_columns, visited_rows, rows_hi_ne_lo,
    max_spread, sorted by line and station. Non-D feature columns are ignored;
    an unknown column name raises ValueError (via build_column_lookup).
    """
    d_columns = _d_columns_by_station(df.columns)

    per_station = []
    for (line, station), columns in sorted(d_columns.items()):
        lo, hi = _row_min_max(columns)
        per_station.append(
            F.struct(
                F.lit(line).alias("line"),
                F.lit(station).alias("station"),
                F.lit(len(columns)).alias("n_d_columns"),
                F.count(lo).alias("visited_rows"),
                F.count(F.when(hi != lo, 1)).alias("rows_hi_ne_lo"),
                F.max(hi - lo).alias("max_spread"),
            )
        )

    # One agg = one pass over the data; the result is a single row holding an
    # array of per-station structs, exploded into one row per station.
    return (
        df.agg(F.array(*per_station).alias("stations"))
        .select(F.explode("stations").alias("s"))
        .select("s.*")
    )
