import pytest
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType

from linewerk.quality.profiling import non_null_counts

SCHEMA = StructType(
    [
        StructField("Id", IntegerType(), nullable=False),
        StructField("L0_S0_F0", DoubleType()),
        StructField("L0_S0_F2", DoubleType()),
        StructField("L1_S24_F1", DoubleType()),
        StructField("Response", IntegerType()),
    ]
)

# 3 parts, sparse like the real data: each part visits only some stations.
ROWS = [
    (1, 0.03, None, None, 0),
    (2, None, -0.1, None, 0),
    (3, 0.08, 0.2, None, 1),
]

FEATURES = ["L0_S0_F0", "L0_S0_F2", "L1_S24_F1"]


@pytest.fixture
def toy_df(spark):
    return spark.createDataFrame(ROWS, SCHEMA)


def test_non_null_counts_single_row_with_names_kept(toy_df):
    result = non_null_counts(toy_df, FEATURES)

    assert result.columns == ["total_rows", *FEATURES]
    assert [row.asDict() for row in result.collect()] == [
        {"total_rows": 3, "L0_S0_F0": 2, "L0_S0_F2": 2, "L1_S24_F1": 0}
    ]


def test_non_null_counts_only_requested_columns(toy_df):
    result = non_null_counts(toy_df, ["L0_S0_F2"])
    assert result.collect()[0].asDict() == {"total_rows": 3, "L0_S0_F2": 2}


def test_non_null_counts_empty_dataframe(spark):
    result = non_null_counts(spark.createDataFrame([], SCHEMA), FEATURES)
    assert result.collect()[0].asDict() == {
        "total_rows": 0,
        "L0_S0_F0": 0,
        "L0_S0_F2": 0,
        "L1_S24_F1": 0,
    }


def test_non_null_counts_rejects_reserved_name(toy_df):
    with pytest.raises(ValueError, match="total_rows"):
        non_null_counts(toy_df, ["total_rows"])
