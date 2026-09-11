import json
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone
from shop.models import Order

class Command(BaseCommand):
    help = "알려진 주문 한 건을 order.created 이벤트 JSON으로 내보냅니다."

    def add_arguments(self, parser):
        parser.add_argument("--order-id", type=int, required=True)

    def handle(self, *args, **options):
        order = Order.objects.get(pk=options["order_id"])
        event = {
            "event_type": "order.created",
            "order_id": order.id,
            "product_id": order.product_id,
            "quantity": order.quantity,
            "unit_price": order.unit_price,
            "amount": order.amount,
            "ordered_at": timezone.localtime(order.ordered_at).isoformat(timespec="seconds"),
        }
        events = settings.DATA_DIR / "events"
        events.mkdir(parents=True, exist_ok=True)
        path = events / "order-created.json"
        path.write_text(json.dumps(event, ensure_ascii=False) + "\n", encoding="utf-8")
        self.stdout.write(f"주문 이벤트 한 건: {path}")