from pyspark.sql import functions as F

from linewerk.config import DATA_DIR
from linewerk.spark import get_spark
from linewerk.ingest.schema import schema_for_file


def explore_1(file_path: str):
    spark = get_spark()

    schema = schema_for_file(file_path)
    df = spark.read.csv(file_path, header=True, schema=schema)

    q1 = df.select("Id", "L0_S0_F0", "Response").filter(F.col("L0_S0_F0") > 0)
    q1.explain("formatted")

    q2 = df.groupBy("Response").count()
    q2.explain("formatted")


def explore_2(numeric_path: str, date_path: str):
    spark = get_spark()
    # spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)   # -1 = broadcast never

    numeric_schema = schema_for_file(numeric_path)
    num_df = spark.read.csv(numeric_path, header=True, schema=numeric_schema).select(
        "Id", "L0_S0_F0"
    )

    date_schema = schema_for_file(date_path)
    date_df = spark.read.csv(date_path, header=True, schema=date_schema).select(
        "Id", "L0_S0_D1"
    )

    q3 = num_df.join(date_df, on="Id")
    q3.explain("formatted")


if __name__ == "__main__":
    sample_numeric_path = DATA_DIR / "sample" / "train_numeric_100k.csv"
    # explore_1(str(sample_numeric_path))

    sample_date_path = DATA_DIR / "sample" / "train_date_100k.csv"
    explore_2(str(sample_numeric_path), str(sample_date_path))
