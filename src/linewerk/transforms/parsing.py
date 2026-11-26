from dataclasses import dataclass

from linewerk.naming import parse_column_by_pattern, NON_FEATURE_COLUMNS


@dataclass(frozen=True)
class ColumnInfo:
    line: int
    station: int
    kind: str
    number: int


def parse_column(column_name: str) -> ColumnInfo:
    parsed_column_name = parse_column_by_pattern(column_name)
    if parsed_column_name is None:
        raise ValueError(f"Unknown column name: {column_name}")

    line, station, kind, number = parsed_column_name.groups()
    return ColumnInfo(int(line), int(station), kind, int(number))


def build_column_lookup(column_names: list[str]) -> list[tuple[str, ColumnInfo]]:
    """Map each feature column name to its parsed (line, station, kind, number).

    Fails fast: an unknown column name raises ValueError from parse_column.
    """
    feature_columns = [c for c in column_names if c not in NON_FEATURE_COLUMNS]
    return [(name, parse_column(name)) for name in feature_columns]
