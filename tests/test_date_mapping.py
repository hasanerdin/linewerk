import pytest
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType

from linewerk.quality.date_mapping import (
    StationNumbers,
    numbers_by_station,
    station_time_spread,
    spread_quantiles,
    global_time_range,
)


def test_numbers_by_station_groups_and_sorts():
    columns = [
        "Id",
        "L0_S1_F24",
        "L0_S0_F2",
        "L0_S0_F0",
        "L0_S0_D3",
        "L0_S0_D1",
        "L0_S1_D25",
        "Response",
    ]
    assert numbers_by_station(columns) == [
        StationNumbers(0, 0, f_numbers=(0, 2), d_numbers=(1, 3)),
        StationNumbers(0, 1, f_numbers=(24,), d_numbers=(25,)),
    ]


def test_unmatched_d_finds_d_without_f_at_offset():
    [station] = numbers_by_station(["L1_S24_F10", "L1_S24_D11", "L1_S24_D20"])
    assert station.unmatched_d(offset=1) == (20,)
    assert station.unmatched_d(offset=0) == (11, 20)


def test_station_with_only_dates_has_empty_f():
    [station] = numbers_by_station(["L3_S51_D4255"])
    assert station.f_numbers == ()
    assert station.unmatched_d(offset=1) == (4255,)


def test_duplicate_feature_column_fails_fast():
    with pytest.raises(ValueError, match="L0_S0_D1"):
        numbers_by_station(["L0_S0_F0", "L0_S0_D1", "L0_S0_D1"])


def test_repeated_id_from_concatenated_headers_is_allowed():
    columns = ["Id", "L0_S0_F0", "Response", "Id", "L0_S0_D1"]
    assert numbers_by_station(columns) == [
        StationNumbers(0, 0, f_numbers=(0,), d_numbers=(1,)),
    ]


def test_unknown_column_fails_fast():
    with pytest.raises(ValueError, match="L0_S0_X1"):
        numbers_by_station(["L0_S0_F0", "L0_S0_X1"])


DATE_SCHEMA = StructType(
    [
        StructField("Id", IntegerType(), nullable=False),
        StructField("L0_S0_D1", DoubleType()),
        StructField("L0_S0_D3", DoubleType()),
        StructField("L0_S0_D5", DoubleType()),
        StructField("L1_S24_D11", DoubleType()),  # single-column station
        StructField("L3_S51_D4255", DoubleType()),  # never visited
    ]
)

DATE_ROWS = [
    (1, 10.0, 10.0, 10.0, None, None),  # all equal
    (2, 20.0, None, 20.5, 30.0, None),  # spread 0.5, one null in between
    (3, 5.0, 7.0, 6.0, 31.0, None),  # spread 2.0, max not in the last column
    (4, None, None, None, None, None),  # visits nothing
]

QUANTILE_SCHEMA = StructType(
    [
        StructField("Id", IntegerType(), nullable=False),
        # S24: disagrees in some rows
        StructField("L1_S24_D1", DoubleType()),
        StructField("L1_S24_D2", DoubleType()),
        StructField("L1_S24_D3", DoubleType()),
        # S25: visited, but its D columns always agree
        StructField("L1_S25_D1", DoubleType()),
        StructField("L1_S25_D2", DoubleType()),
        # S0: disagrees, but is not requested, so must not appear in the output
        StructField("L0_S0_D1", DoubleType()),
        StructField("L0_S0_D2", DoubleType()),
    ]
)

# S24 spreads: 0, 1, 2, 4, 8, 100, 0. Without the two zeros the sorted list is
# [1, 2, 4, 8, 100] -> p50 = 4. With them, p50 would be 2, so the test catches
# a missing hi != lo filter.
# fmt: off
QUANTILE_ROWS = [
    # Id  S24_D1 S24_D2 S24_D3  S25_D1 S25_D2  S0_D1 S0_D2
    (1,   10.0,  10.0,  10.0,   5.0,   5.0,    1.0,  1.0),   # S24 spread 0
    (2,   10.0,  11.0,  None,   7.0,   7.0,    1.0,  9.0),   # S24 spread 1
    (3,   10.0,  12.0,  11.0,   None,  None,   2.0,  11.0),  # S24 spread 2
    (4,   10.0,  None,  14.0,   19.0,  19.0,   2.0,  11.0),  # S24 spread 4
    (5,   10.0,  18.0,  14.0,   None,  None,   2.0,  11.0),  # S24 spread 8, max in the middle
    (6,   10.0,  110.0, None,   39.0,  39.0,   2.0,  11.0),  # S24 spread 100
    (7,   None,  50.0,  50.0,   None,  None,   None, None),  # S24 spread 0
]
# fmt: on


def test_station_time_spread_counts_per_station(spark):
    df = spark.createDataFrame(DATE_ROWS, DATE_SCHEMA)
    result = [row.asDict() for row in station_time_spread(df).collect()]
    assert result == [
        {
            "line": 0,
            "station": 0,
            "n_d_columns": 3,
            "visited_rows": 3,
            "rows_hi_ne_lo": 2,
            "max_spread": 2.0,
        },
        {
            "line": 1,
            "station": 24,
            "n_d_columns": 1,
            "visited_rows": 2,
            "rows_hi_ne_lo": 0,
            "max_spread": 0.0,
        },
        {
            "line": 3,
            "station": 51,
            "n_d_columns": 1,
            "visited_rows": 0,
            "rows_hi_ne_lo": 0,
            "max_spread": None,
        },
    ]


def test_station_time_spread_ignores_f_columns(spark):
    schema = StructType(
        [StructField("L0_S0_F0", DoubleType()), StructField("L0_S0_D1", DoubleType())]
    )
    df = spark.createDataFrame([(99.0, 1.0)], schema)
    [row] = station_time_spread(df).collect()
    assert (row.station, row.n_d_columns, row.max_spread) == (0, 1, 0.0)


def test_station_time_spread_without_d_columns_fails_fast(spark):
    schema = StructType(
        [StructField("Id", IntegerType()), StructField("L0_S0_F0", DoubleType())]
    )
    with pytest.raises(ValueError, match="No D columns"):
        station_time_spread(spark.createDataFrame([], schema))


def test_spread_quantiles_ignores_rows_where_times_agree(spark):
    df = spark.createDataFrame(QUANTILE_ROWS, QUANTILE_SCHEMA)

    result = [
        row.asDict()
        for row in spread_quantiles(df, [(1, 24), (1, 25)], [0.5, 0.95, 0.99]).collect()
    ]

    assert result == [
        {"line": 1, "station": 24, "n_disagree": 5, "quantiles": [4.0, 100.0, 100.0]},
        {"line": 1, "station": 25, "n_disagree": 0, "quantiles": None},
    ]


def test_spread_quantiles_unknown_station_fails_fast(spark):
    df = spark.createDataFrame(QUANTILE_ROWS, QUANTILE_SCHEMA)
    with pytest.raises(ValueError, match="L9_S99"):
        spread_quantiles(df, [(1, 24), (9, 99)], [0.5])


def test_global_time_range_over_all_d_columns(spark):
    # Min 5.0 is not in the first column, L3_S51_D4255 is entirely null, and
    # Id (1..4) is smaller than every D value: reading Id would give min = 1.
    df = spark.createDataFrame(DATE_ROWS, DATE_SCHEMA)
    assert global_time_range(df) == (5.0, 31.0)


def test_global_time_range_ignores_f_columns(spark):
    schema = StructType(
        [
            StructField("L0_S0_F0", DoubleType()),
            StructField("L0_S0_D1", DoubleType()),
            StructField("L0_S0_D3", DoubleType()),
        ]
    )
    df = spark.createDataFrame([(-1.0, 3.0, 4.0), (99.0, 2.0, None)], schema)
    assert global_time_range(df) == (2.0, 4.0)


def test_global_time_range_all_null_fails_fast(spark):
    # DATE_ROWS[3] visits nothing. df[3] would be a column, not a row, so the
    # one-row DataFrame is built from the Python list instead.
    df = spark.createDataFrame([DATE_ROWS[3]], DATE_SCHEMA)
    with pytest.raises(ValueError, match="Every D value is null"):
        global_time_range(df)
