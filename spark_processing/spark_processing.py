from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import os

def main():
    spark = SparkSession.builder \
        .appName("GPU Analysis") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()

    spark.catalog.clearCache()

    schema = StructType([
        StructField("date", StringType(), True),
        StructField("category", StringType(), True),
        StructField("name", StringType(), True),
        StructField("change", DoubleType(), True),
        StructField("percentage", DoubleType(), True)
    ])

    try:
        input_path = "hdfs://localhost:9000/user/infrastructure-project/input/shs.csv"
        print(f"Attempting to read from: {input_path}")

        if not os.system(f"hadoop fs -test -e {input_path}") == 0:
            raise FileNotFoundError(f"Input file not found at {input_path}")

        df = spark.read \
            .option("header", "true") \
            .option("mode", "DROPMALFORMED") \
            .schema(schema) \
            .csv(input_path)

        print(f"Successfully loaded {df.count()} records")

        df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

        gpu_df = df.filter(
            (lower(col("name")).like("%gpu%")) |
            (lower(col("name")).like("%graphics%")) |
            (lower(col("name")).like("%video%"))
        )

        print(f"Filtered to {gpu_df.count()} GPU-related records")

        result = gpu_df.withColumn("year", year("date")) \
            .groupBy("year", "name") \
            .agg(
                avg("percentage").alias("avg_usage"),
                max("percentage").alias("max_usage")
            )

        output_path = "hdfs://localhost:9000/user/infrastructure-project/output"
        result.write.mode("overwrite").parquet(output_path)

        print("\nProcessing complete. Sample results:")
        result.orderBy("year", desc("avg_usage")).show(20, False)

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()