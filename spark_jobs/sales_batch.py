import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
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


preview = [
    row.asDict()
    for row in orders.select("order_id", "product_id", "quantity", "amount")
    .orderBy("order_id")
    .limit(10)
    .collect()
]
summary = {
    "generated_at": datetime.now(ZoneInfo("Asia/Seoul")).isoformat(),
    "order_count": orders.count(),
    "preview": preview,
    "by_product": [row.asDict() for row in by_product.orderBy("product_id").collect()],
    "by_day": [row.asDict() for row in by_day.orderBy("order_date").collect()],
    "page_views": [row.asDict() for row in page_views.orderBy("visit_date").collect()],
    "by_category": [],
}
summary["generated_at"] = datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
output_dir = data_dir / "marts"
output_dir.mkdir(parents=True, exist_ok=True)
with (output_dir / "dashboard.json").open("w", encoding="utf-8") as stream:
    json.dump(summary, stream, ensure_ascii=False, indent=2)


# 집계함수에 파생컬럼을 생성합니다.(.withColumn())
by_product = orders.groupBy("product_id").agg(
    F.count("*").alias("order_count"),
    F.sum("amount").alias("revenue"),
).withColumn(
    "average_order_amount", F.col("revenue") / F.col("order_count")
)

by_product.explain()

by_product.orderBy("product_id").show()


by_product2 = orders.groupBy("product_id").agg(
    F.sum("quantity").alias("sold_quantity")
)

by_product2.explain()

by_product2.show()

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

# 커넥션 끊기
spark.stop()