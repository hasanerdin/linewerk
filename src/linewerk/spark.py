from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip



def get_spark(app_name: str = "linewerk", driver_memory: str = "8g") -> SparkSession:
    """
        Get a SparkSession with Delta Lake support.
        Args:
            app_name (str): The name of the Spark application. Default is "linewerk".
            driver_memory (str): The amount of memory to allocate for the Spark driver. Default is "8g".
        Returns:
            SparkSession: A SparkSession object with Delta Lake support. 
            driver_memory only takes effect when the JVM is first started; 
            later calls return the existing session and ignore it.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config(
            "spark.sql.extensions", 
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark