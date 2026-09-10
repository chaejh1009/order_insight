import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType,StructField,LongType,StringType,IntegerType,)

parser = argparse.ArgumentParser()

parser.add_argument(
    "--data-dir",
    required=True,
)

args = parser.parse_args()

data_dir = Path(args.data_dir).resolve()


spark = (
    SparkSession.builder
    .appName("order-insight-delta-changes")
    .config("spark.sql.session.timeZone", "Asia/Seoul")
    .getOrCreate()
)

# INFO 레벨 로그 안 보기
spark.sparkContext.setLogLevel("WARN")

order_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", LongType(), True),
    StructField("ordered_at", StringType(), True),
])

raw_orders = (
    spark.read
    .option("header", True)
    .schema(order_schema)
    .csv(str(data_dir / "raw" / "orders.csv"))
)

orders = (
    raw_orders
    .filter(F.col("order_id") <= 12)
    .withColumn(
        "amount",
        F.col("quantity") * F.col("unit_price"),
    )
    .withColumn(
        "ordered_at",
        F.to_timestamp("ordered_at"),
    )
    .withColumn(
        "order_date",
        F.to_date("ordered_at"),
    )
)


demo_path = data_dir / "demo" / "delta_orders"


first_day = orders.filter(
    F.col("order_date") == F.lit("2026-09-07").cast("date")
)

next_day = orders.filter(
    F.col("order_date") == F.lit("2026-09-08").cast("date")
)

first_day.write \
    .format("delta") \
    .mode("overwrite") \
    .save(str(demo_path))

print(
    "after overwrite:",
    spark.read.format("delta").load(str(demo_path)).count(),
)
print("=" * 20, "9월 7일 데이터만 업로드", "=" * 20)
spark.read.format("delta").load(str(demo_path)).show()
next_day.write \
    .format("delta") \
    .mode("append") \
    .save(str(demo_path))

print(
    "after append:",
    spark.read.format("delta").load(str(demo_path)).count(),
)
print("=" * 20, "9월 8일 데이터 추가 업로드", "=" * 20)
spark.read.format("delta").load(str(demo_path)).show()
spark.sql(
    f"""
    UPDATE delta.`{demo_path}`
    SET
        quantity = 2,
        amount = unit_price * 2
    WHERE order_id = 1
    """
)
print("=" * 20, "1번 구매기록 변경 후", "=" * 20)
spark.read.format("delta").load(str(demo_path)).show()


current_orders = spark.read.format("delta").load(str(demo_path))
current_orders.orderBy("order_id").show()
current_orders.groupBy("product_id").agg(
    F.sum("amount").alias("revenue")
).orderBy("product_id").show()

history = spark.sql(f"DESCRIBE HISTORY delta.`{demo_path}`")

history.select(
    "version",
    "operation",
    "operationParameters",
).orderBy(
    F.col("version").desc()
).show(
    truncate=False
)

current_orders.filter(F.col("order_id") == 1).select(
    "order_id", "quantity", "amount"
).show()

spark.stop()