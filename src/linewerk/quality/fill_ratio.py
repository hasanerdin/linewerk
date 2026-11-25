"""Fill ratios per station and per line, from cached non-null counts.

Pure Python on purpose: the input is one number per feature (968 of them),
so Spark would only add start-up cost here.
"""

from collections import defaultdict
from dataclasses import dataclass

from linewerk.transforms.parsing import parse_column


@dataclass(frozen=True)
class StationFill:
    line: int
    station: int
    n_features: int
    min_non_null: int
    max_non_null: int
    filled_cells: int
    fill_ratio: float

    @property
    def all_or_nothing(self) -> bool:
        """Every feature of the station has the same non-null count.

        Necessary for "filled together / empty together", not sufficient:
        equal counts could still come from different rows.
        """
        return self.min_non_null == self.max_non_null


@dataclass(frozen=True)
class LineFill:
    line: int
    n_features: int
    filled_cells: int
    fill_ratio: float


def _ratio(filled: int, total_rows: int, n_features: int) -> float:
    cells = total_rows * n_features
    return filled / cells if cells else 0.0


def station_fill(non_null: dict[str, int], total_rows: int) -> list[StationFill]:
    """One StationFill per (line, station), sorted by line then station."""
    counts: dict[tuple[int, int], list[int]] = defaultdict(list)
    for column, n in non_null.items():
        info = parse_column(column)
        counts[(info.line, info.station)].append(n)

    return [
        StationFill(
            line=line,
            station=station,
            n_features=len(ns),
            min_non_null=min(ns),
            max_non_null=max(ns),
            filled_cells=sum(ns),
            fill_ratio=_ratio(sum(ns), total_rows, len(ns)),
        )
        for (line, station), ns in sorted(counts.items())
    ]


def line_fill(non_null: dict[str, int], total_rows: int) -> list[LineFill]:
    """One LineFill per line, sorted by line."""
    counts: dict[int, list[int]] = defaultdict(list)
    for column, n in non_null.items():
        counts[parse_column(column).line].append(n)

    return [
        LineFill(
            line=line,
            n_features=len(ns),
            filled_cells=sum(ns),
            fill_ratio=_ratio(sum(ns), total_rows, len(ns)),
        )
        for line, ns in sorted(counts.items())
    ]


def total_filled_cells(non_null: dict[str, int]) -> int:
    """All non-null feature cells = expected row count of the long (melted) table."""
    return sum(non_null.values())
