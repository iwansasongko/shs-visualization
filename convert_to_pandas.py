from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Convert Spark DataFrame to Pandas") \
    .getOrCreate()

output_path = "output/part-00000-b651fea1-c084-45d3-811e-c40399c76500-c000.snappy.parquet"
df = spark.read.parquet(output_path)

pandas_df = df.toPandas()

pandas_df.to_csv("gpu_analysis.csv", index=False)

print("Data berhasil dikonversi ke Pandas dan disimpan ke 'gpu_analysis.csv'")