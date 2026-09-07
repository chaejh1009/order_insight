import subprocess
from pathlib import Path
from time import perf_counter

from django.conf import settings
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "설치된 Spark 배포판의 spark-submit으로 배치를 제출합니다."

    def add_arguments(self, parser):
        parser.add_argument("--cores", type=int, choices=[1, 2], default=2)
        parser.add_argument("--script", default="spark_jobs/sales_batch.py")
        parser.add_argument("--data-dir", default=str(settings.DATA_DIR))

    def handle(self, *args, **options):
        script = (settings.BASE_DIR / options["script"]).resolve()
        data_dir = Path(options["data_dir"]).resolve()
        command = [
            settings.SPARK_SUBMIT,
            "--deploy-mode", "client",
            "--executor-cores", "1",
            "--total-executor-cores", str(options["cores"]),
        ]
        command += [str(script), "--data-dir", str(data_dir)]
        started = perf_counter()
        subprocess.run(command, cwd=settings.BASE_DIR, check=True)
        elapsed = perf_counter() - started
        self.stdout.write(f"Spark 제출부터 프로세스 종료까지: {elapsed:.2f}초")