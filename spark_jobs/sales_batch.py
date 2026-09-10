import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType, TimestampType
# 5일차 7교시 추가코드
import json
from datetime import datetime
from zoneinfo import ZoneInfo
# 7일차 5교시 추가코드
from time import perf_counter



parser = argparse.ArgumentParser()
parser.add_argument("--data-dir", required=True)
args = parser.parse_args()
data_dir = Path(args.data_dir).resolve()

# 스파크와 파이썬 코드 마운트하기(커넥션 맺기)
spark = (
    SparkSession.builder.appName("order-insight-sales")
    .config("spark.sql.session.timeZone", "Asia/Seoul")
    .getOrCreate()
)

# 스파크 연결 후 정제 시작시간
started = perf_counter()

# INFO 레벨 로그 안 보기
spark.sparkContext.setLogLevel("WARN")

# 주문 스키마 설정하기
order_schema = StructType([
    StructField("order_id", LongType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", LongType()),
    StructField("ordered_at", StringType()),
])

# product schema는 단순하므로, 문자로 나열하고 끝냅니다.
product_schema = "product_id string, name string, category string"

# csv파일 읽어오기.
raw_orders = spark.read.schema(order_schema).option("header", True).csv(
    (data_dir / "raw" / "orders.csv").as_uri()
)

# json파일 읽어오기.
products = spark.read.schema(product_schema).json(
    (data_dir / "raw" / "products.jsonl").as_uri()
)

# 읽어온 raw데이터 정제하기
orders = (
    raw_orders.withColumn("amount", F.col("quantity") * F.col("unit_price"))
    .withColumn("ordered_at", F.to_timestamp("ordered_at"))
    .withColumn("order_date", F.to_date("ordered_at"))
)

# orders에서 특정 컬럼만 조회하기.
orders.select("order_id", "product_id", "quantity", "amount", "order_date").show()

# where절처럼 필터링한 후, .select로 조회하기.
orders.filter(F.col("product_id") == "B").select("quantity", "amount", "order_date").show()

# products는 조건 없이 그냥 다 조회해서 보여주기.
products.show()




# 집계함수를 활용해서 상품별 매출총액을 구합니다.
by_product = orders.groupBy("product_id").agg(
    F.count("*").alias("order_count"),
    F.sum("amount").alias("revenue"),
)
by_product.orderBy("product_id").show()


by_day = orders.groupBy("order_date").agg(
    F.count("*").alias("order_count"),
    F.sum("amount").alias("revenue"),
).withColumn("order_date", F.col("order_date").cast("string"))
print("="*20, "날짜별 매출액", "="*20)
by_day.orderBy("order_date").show()

logs = spark.read.text((data_dir / "raw" / "access.log").as_uri())
#logs.show(truncate=False)
log_pattern = r"^(\S+) (\S+) (\S+) (\d+) (\d+)$"

extracted = logs.select(
    F.regexp_extract("value", log_pattern, 1).alias("requested_at_text"),
    F.regexp_extract("value", log_pattern, 2).alias("method"),
    F.regexp_extract("value", log_pattern, 3).alias("path"),
    F.regexp_extract("value", log_pattern, 4).alias("status_text"),
    F.regexp_extract("value", log_pattern, 5).alias("duration_text"),
)
extracted.show(truncate=False)

parsed = logs.select(
    F.to_timestamp(F.regexp_extract("value", log_pattern, 1)).alias("requested_at"),
    F.regexp_extract("value", log_pattern, 2).alias("method"),
    F.regexp_extract("value", log_pattern, 3).alias("path"),
    F.regexp_extract("value", log_pattern, 4).cast("int").alias("status"),
    F.regexp_extract("value", log_pattern, 5).cast("long").alias("duration_ms"),
).withColumn("visit_date", F.date_format("requested_at", "yyyy-MM-dd"))
print("="*20, "가공된 일자별 개별 로그", "="*20)
parsed.select("visit_date", "path", "status", "duration_ms").show()

page_views = parsed.groupBy("visit_date").agg(
    F.count("*").alias("page_views"),
    F.sum("duration_ms").alias("total_duration_ms"),
)
print("="*20, "날짜별 방문트래픽수", "="*20)
page_views.orderBy("visit_date").show()




print("=" * 50, "조인된 데이터로 집계하기 시작", "=" * 50)
enriched = orders.join(F.broadcast(products), on="product_id", how="inner")
enriched.select("order_id", "product_id", "category", "amount").orderBy("order_id").show()
# 집계함수에 파생컬럼을 생성합니다.(.withColumn())
by_product = orders.groupBy("product_id").agg(
    F.count("*").alias("order_count"),
    F.sum("amount").alias("revenue"),
).withColumn(
    "average_order_amount", F.col("revenue") / F.col("order_count")
)

by_product = enriched.groupBy("product_id", "name", "category").agg(
    F.sum("amount").alias("revenue"),
    F.count("*").alias("order_count"),
).withColumnRenamed("name", "product_name").withColumn(
    "average_order_amount", F.col("revenue") / F.col("order_count")
)

# by_product.explain()

by_product.filter(F.col("category") == "음료").orderBy("product_id").show()
print("=" * 50, "조인된 데이터로 집계하기 끝", "=" * 50)


by_category = enriched.groupBy("category").agg(
    F.count("*").alias("order_count"),
    F.sum("amount").alias("revenue"),
)

category_rows = by_category.orderBy("category").collect()
print([row.asDict() for row in category_rows])

# 추가 지표 입력 예시용
overall = orders.agg(
    F.count("*").alias("order_count"),
    F.sum("amount").alias("total_revenue"),
    F.avg("amount").alias("average_order_amount"),
    F.max("amount").alias("max_order_amount"),
    F.min("amount").alias("min_order_amount"),
).first().asDict()


# by_product2 = orders.groupBy("product_id").agg(
#     F.sum("quantity").alias("sold_quantity")
# )

# # by_product2.explain()

# by_product2.show()

# # 시간 측정을 위한 csv파일 가져오기.
# started = perf_counter()
# measured_orders = (
#     spark.read.schema(order_schema).option("header", True)
#     .csv((data_dir / "raw" / "orders.csv").as_uri())
#     .withColumn("amount", F.col("quantity") * F.col("unit_price"))
# )
# # 가져온 csv파일로 집계연산 후 종료하면서 시간측정.
# measured_summary = measured_orders.groupBy("product_id").agg(
#     F.sum("amount").alias("revenue"),
# )
# result = measured_summary.collect()
# print("읽기·집계·결과 수신 초:", perf_counter() - started)
# print(result)

# 7일차 관찰 시작
# sample_orders = orders.filter(F.col("order_id") <= 12)
# #print("=" * 50)
# #print("all_rows =", orders.count())
# #print("=" * 50)
# #print("sample_rows =", sample_orders.count())
# print("=" * 50)
# sample_orders.select("order_id", "product_id", "amount").orderBy("order_id").show()

# #print("=" * 50, "파티션 개수 조회 시작", "=" * 50)
# #print("input_partitions =", sample_orders.rdd.getNumPartitions())

# print("=" * 50, "파티션 세부 데이터 조회", "=" * 50)
# partition_rows = sample_orders.select(
#     "order_id",
#     "product_id",
#     "amount",
#     F.spark_partition_id().alias("partition_id"),
# )
# partition_rows.show()

# print("=" * 50, "파티션별 할당 데이터 개수 조회", "=" * 50)
# partition_sizes = partition_rows.groupBy("partition_id").agg(
#     F.count("*").alias("row_count")
# )
# partition_sizes.orderBy("partition_id").show()

# print("=" * 50, "orders 데이터에 대한 4개 파티션 분할여부 확인", "=" * 50)
# split_orders = sample_orders.repartition(4)
# print("split_partitions =", split_orders.rdd.getNumPartitions())

# print("=" * 50, "파티션 배정 여부 확인과 파티션별 세부 데이터 확인.", "=" * 50)
# split_rows = split_orders.select(
#     "order_id", "product_id", F.spark_partition_id().alias("partition_id")
# )
# split_rows.show()
# split_rows.groupBy("partition_id").agg(
#     F.count("*").alias("row_count")
# ).orderBy("partition_id").show()

# print("=" * 50, "나눠진 파티션 병합하기.", "=" * 50)
# merged_orders = split_orders.coalesce(2)
# print("merged_partitions =", merged_orders.rdd.getNumPartitions())

# # print("=" * 50, "파티션 분할 정도별 계획 확인", "=" * 50)
# # print("split plan")
# # split_orders.explain("formatted")
# # print("merged plan")
# # merged_orders.explain("formatted")

# print("=" * 50, "병합전후 파티션의 실제 데이터 row수가 동일함을 확인.", "=" * 50)
# print("split_rows =", split_orders.count())
# print("merged_rows =", merged_orders.count())
# merged_orders.select(
#     "order_id", "product_id", F.spark_partition_id().alias("partition_id")
# ).show()

# print("=" * 50, "파티션 개수가 달라도 결과는 달라지지 않음을 확인.", "=" * 50)
# split_sales = split_orders.groupBy("product_id").agg(
#     F.sum("amount").alias("revenue")
# )
# merged_sales = merged_orders.groupBy("product_id").agg(
#     F.sum("amount").alias("revenue")
# )
# split_sales.orderBy("product_id").show()
# merged_sales.orderBy("product_id").show()

# print("=" * 50, "sample주문을 상품별로 묶고, 주문횟수와 총매출 계산", "=" * 50)
# sample_summary = sample_orders.groupBy("product_id").agg(
#     F.count("*").alias("order_count"),
#     F.sum("amount").alias("revenue"),
# )
# # sample_summary.explain("formatted")
# sample_summary.orderBy("product_id").show()

# keyed_orders = sample_orders.repartition(4, "product_id")
# print("keyed_partitions =", keyed_orders.rdd.getNumPartitions())
# print("=" * 50, "키로 지정한 product_id별로 분류되어 파티션 배정이 되었는지 확인.", "=" * 50)
# keyed_rows = keyed_orders.select(
#     "order_id", "product_id", F.spark_partition_id().alias("partition_id")
# )
# keyed_rows.show()
# keyed_sizes = keyed_rows.groupBy("partition_id", "product_id").agg(
#     F.count("*").alias("row_count")
# )
# keyed_sizes.orderBy("partition_id", "product_id").show()

# print("=" * 50, "나눠진 파티션으로 집계하기.", "=" * 50)
# keyed_orders.groupBy("product_id").agg(
#     F.count("*").alias("order_count"),
#     F.sum("amount").alias("revenue"),
# ).orderBy("product_id").show()


# print("=" * 50, "조인 대상 테이블 상태 점검.", "=" * 50)
# sample_orders.select("order_id", "product_id", "quantity", "amount").orderBy("order_id").show()
# products.select("product_id", "name", "category").orderBy("product_id").show()

# print("=" * 50, "product_id기반 조인 결과 테이블 구조 확인 및 row 개수 확인.", "=" * 50)
# sample_enriched = sample_orders.join(products, on="product_id", how="inner")
# sample_enriched.printSchema()
# print("joined_rows =", sample_enriched.count())

# print("=" * 50, "조인된 결과 테이블 조회", "=" * 50)
# sample_named_rows = sample_enriched.select(
#     "order_id", "product_id", "name", "category", "amount"
# ).withColumnRenamed("name", "product_name")
# sample_named_rows.orderBy("order_id").show()

# print("=" * 50, "조인 후, 상품별 집계", "=" * 50)
# sample_by_product = sample_enriched.groupBy("product_id", "name").agg(
#     F.count("*").alias("order_count"),
#     F.sum("amount").alias("revenue"),
# ).withColumnRenamed("name", "product_name")
# sample_by_product.orderBy("product_id").show()

# print("=" * 50, "힌트 없는 조인", "=" * 50)
# normal_join = sample_orders.join(products, "product_id", "inner")
# print("normal join plan")
# normal_join.explain("formatted")

# print("=" * 50, "힌트 있는 조인", "=" * 50)
# broadcast_join = sample_orders.join(F.broadcast(products), "product_id", "inner")
# print("broadcast join plan")
# broadcast_join.explain("formatted")


# normal_join.select("order_id", "product_id", "name", "amount").orderBy("order_id").show()
# broadcast_join.select("order_id", "product_id", "name", "amount").orderBy("order_id").show()


# normal_sales = normal_join.groupBy("product_id").agg(
#     F.sum("amount").alias("revenue")
# )
# broadcast_sales = broadcast_join.groupBy("product_id").agg(
#     F.sum("amount").alias("revenue")
# )
# normal_sales.orderBy("product_id").show()
# broadcast_sales.orderBy("product_id").show()
# 7일차 관찰 끝

# parquet 시작
raw_orders = spark.read.schema(order_schema).option("header", True).csv(
    (data_dir / "raw" / "orders.csv").as_uri()
)

bronze_path = (data_dir / "lake" / "bronze" / "orders").as_uri()

raw_orders.orderBy("order_id").show()
raw_orders.write.mode("overwrite").parquet(bronze_path)

## 용량 줄어드는 예시
# # 주문 스키마 설정하기
# olist_order_items_schema = StructType([
#     StructField("order_id", StringType()),
#     StructField("order_item_id", IntegerType()),
#     StructField("product_id", StringType()),
#     StructField("seller_id", StringType()),
#     StructField("shipping_limit_date", TimestampType()),
#     StructField("price", DecimalType(10, 2)),
#     StructField("freight_value", DecimalType(10, 2)),
# ])

# olist_order_items = spark.read.schema(olist_order_items_schema).option("header", True).csv(
#     (data_dir / "raw" / "olist_order_items_dataset.csv").as_uri()
# )

# olist_order_items_bronze_path = (data_dir / "lake" / "bronze" / "olist_order_items").as_uri()

# olist_order_items.orderBy("order_id", "order_item_id").show()
# olist_order_items.write.mode("overwrite").parquet(olist_order_items_bronze_path)

bronze_orders = spark.read.parquet(bronze_path)

products = spark.read.schema(product_schema).json(
    (data_dir / "raw" / "products.jsonl").as_uri()
)

orders = (
    bronze_orders.withColumn("amount", F.col("quantity") * F.col("unit_price"))
    .withColumn("ordered_at", F.to_timestamp("ordered_at"))
    .withColumn("order_date", F.to_date("ordered_at"))
)

bronze_orders.printSchema()
selected = bronze_orders.select("product_id", "quantity", "unit_price")
selected.explain("formatted")

silver_path = (data_dir / "lake" / "silver" / "orders").as_uri()
orders = spark.read.format("delta").load(silver_path)

# bronze_orders.printSchema()
# orders.printSchema()
orders.filter(F.col("order_id") <= 12).groupBy("order_date").agg(
    F.sum("amount").alias("revenue")
).orderBy("order_date").show()

orders.write.format("delta").mode("overwrite").save(silver_path)
saved_orders = spark.read.format("delta").load(silver_path)
saved_orders.select("order_id", "quantity", "amount").orderBy("order_id").show()

preview = [
    row.asDict()
    for row in orders.select("order_id", "product_id", "quantity", "amount")
    .orderBy("order_id")
    .limit(10)
    .collect()
]
summary = {
    "generated_at": datetime.now(ZoneInfo("Asia/Seoul")).isoformat(),
    "overall": overall,
    "order_count": orders.count(),
    "preview": preview,
    "by_product": [row.asDict() for row in by_product.orderBy("product_id").collect()],
    "by_day": [row.asDict() for row in by_day.orderBy("order_date").collect()],
    "page_views": [row.asDict() for row in page_views.orderBy("visit_date").collect()],
    "by_category": [row.asDict() for row in by_category.orderBy("category").collect()],
}
summary["generated_at"] = datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
# 소요시간 측정 마감 및 pipeline_seconds에 저장.
summary["pipeline_seconds"] = round(perf_counter() - started, 3)
output_dir = data_dir / "marts"
output_dir.mkdir(parents=True, exist_ok=True)
with (output_dir / "dashboard.json").open("w", encoding="utf-8") as stream:
    json.dump(summary, stream, ensure_ascii=False, indent=2)
print("orders =", summary["order_count"])
print("pipeline_seconds =", summary["pipeline_seconds"])
# 커넥션 끊기
spark.stop()
