from pathlib import Path
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, DataType

from linewerk.config import RAW_DIR
from linewerk.naming import check_column_pattern


def read_header(path: Path) -> list[str]:
    with open(path, mode="r") as file:
        header = file.readline()

    return header.strip().split(",")


def find_the_column_type(column_name: str) -> DataType:
    if column_name in ["Id", "Response"]:
        return IntegerType()

    if check_column_pattern(column_name):
        return DoubleType()
    else:
        raise ValueError(f"Unknown column name: {column_name}")


def build_schema(columns: list[str]) -> StructType:
    return StructType(
        [
            StructField(
                column_name,
                find_the_column_type(column_name),
                nullable=False if column_name == "Id" else True,
            )
            for column_name in columns
        ]
    )


def schema_for_file(path: Path) -> StructType:
    columns = read_header(path)
    return build_schema(columns)


if __name__ == "__main__":
    train_numeric_struct_types = schema_for_file(RAW_DIR / "train_numeric.csv")
    print("Train Numeric: ")
    print("Total column number: ", len(train_numeric_struct_types))
    print(
        train_numeric_struct_types.fieldNames()[:3]
        + train_numeric_struct_types.fieldNames()[-3:]
    )

    train_date_struct_types = schema_for_file(RAW_DIR / "train_date.csv")
    print("Train Date: ")
    print("Total column number: ", len(train_date_struct_types))
    print(
        train_date_struct_types.fieldNames()[:3]
        + train_date_struct_types.fieldNames()[-3:]
    )
