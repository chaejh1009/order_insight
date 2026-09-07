import csv
import json
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from shop.models import Order, Product


class Command(BaseCommand):
    help = "현재 주문과 상품을 Spark 입력 CSV·JSONL로 내보냅니다."

    def add_arguments(self, parser):
        parser.add_argument("--data-dir", default=str(settings.DATA_DIR))

    def handle(self, *args, **options):
        raw = Path(options["data_dir"]).resolve() / "raw"
        raw.mkdir(parents=True, exist_ok=True)
        with (raw / "orders.csv").open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["order_id", "product_id", "quantity", "unit_price", "ordered_at"])
            for order in Order.objects.order_by("id").iterator():
                writer.writerow([
                    order.id,
                    order.product_id,
                    order.quantity,
                    order.unit_price,
                    timezone.localtime(order.ordered_at).isoformat(timespec="seconds"),
                ])

        with (raw / "products.jsonl").open("w", encoding="utf-8") as stream:
            for product in Product.objects.order_by("id"):
                row = {"product_id": product.id, "name": product.name, "category": product.category}
                stream.write(json.dumps(row, ensure_ascii=False) + "\n")

        log_text = (settings.DATA_DIR / "raw" / "access.log").read_text(encoding="utf-8")
        (raw / "access.log").write_text(log_text, encoding="utf-8")
        self.stdout.write(f"주문·상품·접속 로그 내보내기: {raw}")