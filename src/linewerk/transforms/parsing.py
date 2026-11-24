from dataclasses import dataclass

from linewerk.naming import parse_column_by_pattern

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
