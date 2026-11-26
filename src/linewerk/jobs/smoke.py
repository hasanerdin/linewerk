from linewerk.spark import get_spark
from delta.tables import DeltaTable

table_path = "./data/lake/_smoke"

df1_rows = [(123, "station1", 12.3), (456, "station 2", 4.56), (789, "station 1", 7.8)]

df2_rows = [
    (234, "station2", 23.4),
    (760, "station 3", 7.6),
]


def main():
    spark = get_spark()

    df1 = spark.createDataFrame(
        df1_rows, schema="part_id INT, station STRING, value DOUBLE"
    )
    df1.write.format("delta").mode("overwrite").save(table_path)

    df2 = spark.createDataFrame(
        df2_rows, schema="part_id INT, station STRING, value DOUBLE"
    )
    df2.write.format("delta").mode("append").save(table_path)

    table = spark.read.format("delta").load(table_path)
    assert table.count() == 5, "Total number of rows is wrong! It should be 5."

    table.show()

    df = DeltaTable.forPath(spark, table_path).history()
    df["version", "operation"].show()


if __name__ == "__main__":
    main()
