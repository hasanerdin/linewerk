import time
from contextlib import contextmanager

from linewerk.config import DATA_DIR, RAW_DIR
from linewerk.spark import get_spark
from linewerk.ingest.schema import schema_for_file


@contextmanager
def time_tracker(label: str):
    start = time.perf_counter()
    try:
        yield
    finally:
        end = time.perf_counter()
        print(f"[{label}] Time: {end - start: .4f} s")


def main(file_path: str):
    spark = get_spark()

    # JVM, Catalyst ve Executer raise up
    spark.range(1000).selectExpr("sum(id)").collect()

    with time_tracker("Explicit CSV Read"):
        schema = schema_for_file(file_path)
        df_sample = spark.read.csv(file_path, header=True, schema=schema)
        print("Total explicit rows: ", df_sample.count())

    with time_tracker("Inferred CSV Read"):
        df_sample_infer = spark.read.csv(file_path, header=True, inferSchema=True)
        print("Total inferred rows: ", df_sample_infer.count())

    unmatch_counter = 0
    equal_counter = 0
    not_equal_counter = 0
    explicit_types = set()
    implicit_types = set()
    for (column_name, column_type), (column_infer_name, column_infer_type) in zip(
        df_sample.dtypes, df_sample_infer.dtypes
    ):
        explicit_types.add(column_type)
        implicit_types.add(column_infer_type)

        if column_name == column_infer_name:
            if column_type != column_infer_type:
                not_equal_counter += 1
                print(
                    f"[Unmatched] Column Name: {column_name}, Explicit Type: {column_type}, Inferred Type: {column_infer_type}"
                )
            else:
                equal_counter += 1
        else:
            unmatch_counter += 1

    print(
        f"Unmatched: {unmatch_counter} \n Equal: {equal_counter} \n Not Equal: {not_equal_counter}"
    )
    print(f"Explicit Types: {explicit_types}")
    print(f"Inferred Types: {implicit_types}")

    input("Spark UI: http://localhost:4040 - Enter to exit")


if __name__ == "__main__":
    sample_file_path = DATA_DIR / "sample" / "train_numeric_100k.csv"
    file_path = RAW_DIR / "train_numeric.csv"
    main(str(file_path))
