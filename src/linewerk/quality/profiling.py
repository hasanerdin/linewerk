from pyspark.sql import DataFrame
from pyspark.sql import functions as F

TOTAL_ROWS_COLUMN = "total_rows"


def non_null_counts(df: DataFrame, columns: list[str]) -> DataFrame:
    """One-row DataFrame: total_rows + the non-null count of every given column.

    All counts are computed in a single aggregation (one pass over the data).
    F.count(col) skips nulls, F.count(lit(1)) counts every row. Each result
    column keeps the input column's name via alias.
    """
    if TOTAL_ROWS_COLUMN in columns:
        raise ValueError(f"'{TOTAL_ROWS_COLUMN}' is reserved for the row count")

    return df.agg(
        F.count(F.lit(1)).alias(TOTAL_ROWS_COLUMN),
        *[F.count(F.col(c)).alias(c) for c in columns],
    )
