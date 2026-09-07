import json

from django.conf import settings
from django.shortcuts import render


def dashboard(request):
    path = settings.DATA_DIR / "marts" / "dashboard.json"
    summary = json.loads(path.read_text(encoding="utf-8"))
    return render(request, "analytics/dashboard.html", {"summary": summary})