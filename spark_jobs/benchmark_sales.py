import argparse
from pathlib import Path
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--data-dir", required=True)
args = parser.parse_args()
data_dir = Path(args.data_dir).resolve()

spark = (
    SparkSession.builder.appName("order-insight-benchmark")
    .config("spark.sql.session.timeZone", "Asia/Seoul")
    .config("spark.sql.files.maxPartitionBytes", 8 * 1024 * 1024)
    .config("spark.sql.files.minPartitionNum", 4)
    .getOrCreate()
)

started = perf_counter()
orders = (
    spark.read.schema(
        "order_id long, product_id string, quantity int, unit_price long, ordered_at string"
    )
    .option("header", True)
    .csv((data_dir / "raw" / "orders.csv").as_uri())
    .withColumn("amount", F.col("quantity") * F.col("unit_price"))
)

summary_df = orders.groupBy("product_id").agg(
    F.count("*").alias("order_count"),
    F.sum("amount").alias("revenue"),
)
result = summary_df.collect()
elapsed = perf_counter() - started

print("read_and_aggregate_seconds =", round(elapsed, 3))
print("product_count =", len(result))
print("order_count =", sum(row.order_count for row in result))
print("revenue =", sum(row.revenue for row in result))
print("input_partitions =", orders.rdd.getNumPartitions())
print(sorted((row.product_id, row.order_count, row.revenue) for row in result))
summary_df.explain("formatted")
spark.stop()