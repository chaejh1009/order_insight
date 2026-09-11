import argparse
import csv
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--data-dir", default="data-large")
parser.add_argument("--rows", type=int, default=1_000_000)
args = parser.parse_args()

data_dir = Path(args.data_dir).resolve()
raw = data_dir / "raw"
raw.mkdir(parents=True, exist_ok=True)

with (raw / "orders.csv").open("w", encoding="utf-8", newline="") as target:
    writer = csv.writer(target)
    writer.writerow(["order_id", "product_id", "quantity", "unit_price", "ordered_at"])

    for index in range(args.rows):
        writer.writerow([
            index + 1,
            f"P{index % 100:03}",
            index % 5 + 1,
            (index % 10 + 1) * 100,
            "2026-09-07T10:00:00+09:00",
        ])

with (raw / "products.jsonl").open("w", encoding="utf-8") as target:
    for index in range(100):
        row = {
            "product_id": f"P{index:03}",
            "name": f"상품 {index:03}",
            "category": f"분류 {index % 5}",
        }
        target.write(json.dumps(row, ensure_ascii=False) + "\n")

(raw / "access.log").write_text(
    "2026-09-07T10:00:00+09:00 GET /products/ 200 12\n", encoding="utf-8"
)
print("synthetic snapshot =", data_dir, "rows =", args.rows)