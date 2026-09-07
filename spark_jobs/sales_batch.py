import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
# 5일차 7교시 추가코드
import json
from datetime import datetime
from zoneinfo import ZoneInfo


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
    "by_product": [],
    "by_day": [],
    "page_views": [],
    "by_category": [],
}
output_dir = data_dir / "marts"
output_dir.mkdir(parents=True, exist_ok=True)
with (output_dir / "dashboard.json").open("w", encoding="utf-8") as stream:
    json.dump(summary, stream, ensure_ascii=False, indent=2)


# 커넥션 끊기
spark.stop()