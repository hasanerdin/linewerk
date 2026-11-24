import re

def check_column_pattern(column_name: str) -> bool:
    return bool(parse_column_by_pattern(column_name))

def parse_column_by_pattern(column_name: str) -> re.Match[str] | None:
    return re.fullmatch(r"L(\d+)+_S(\d+)+_([FD])(\d+)+", column_name)