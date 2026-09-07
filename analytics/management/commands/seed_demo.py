import json

from django.conf import settings
from django.core.management import call_command
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "수업 시작 시 상품 2개·주문 12건과 접속 로그·빈 대시보드를 준비합니다."

    def handle(self, *args, **options):
        call_command("loaddata", "demo", verbosity=0)
        raw = settings.DATA_DIR / "raw"
        marts = settings.DATA_DIR / "marts"
        raw.mkdir(parents=True, exist_ok=True)
        marts.mkdir(parents=True, exist_ok=True)
        (raw / "access.log").write_text(
            "2026-09-07T10:15:00+09:00 GET /products/ 200 12\n"
            "2026-09-07T10:20:00+09:00 GET /products/ 200 8\n"
            "2026-09-07T11:00:00+09:00 GET /products/ 200 10\n"
            "2026-09-08T09:00:00+09:00 GET /products/ 200 9\n"
            "2026-09-08T10:30:00+09:00 GET /products/ 200 11\n"
            "2026-09-08T12:00:00+09:00 GET /products/ 200 7\n",
            encoding="utf-8",
        )
        summary = {
            "generated_at": "아직 집계 전",
            "order_count": 0,
            "preview": [],
            "by_product": [],
            "by_day": [],
            "page_views": [],
            "by_category": [],
        }
        (marts / "dashboard.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        self.stdout.write("상품 2개·주문 12건, 접속 로그 6건, 초기 대시보드를 준비했습니다.")