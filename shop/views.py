from time import perf_counter

from django.conf import settings
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt

from .models import Order, Product


def products(request):
    started = perf_counter()
    if request.method == "POST":
        product = Product.objects.get(pk=request.POST["product_id"])
        Order.objects.create(
            product=product,
            quantity=int(request.POST["quantity"]),
            unit_price=product.unit_price,
            ordered_at=timezone.now(),
        )
        return redirect("shop:orders")

    response = render(request, "shop/products.html", {"products": Product.objects.order_by("id")})
    duration_ms = int((perf_counter() - started) * 1000)
    timestamp = timezone.localtime().isoformat(timespec="seconds")
    with (settings.DATA_DIR / "raw" / "access.log").open("a", encoding="utf-8") as stream:
        stream.write(f"{timestamp} GET /products/ 200 {duration_ms}\n")
    return response


def orders(request):
    recent_orders = Order.objects.select_related("product").order_by("-id")[:50]
    return render(request, "shop/orders.html", {"orders": recent_orders})

def formview(request):
    return render(request, "shop/form.html")

@csrf_exempt
def formprocess(request):
    data = request.POST["test1"]
    data2 = request.POST["encore"]
    print("당신이 폼으로 보낸 데이터 : ", data)
    print("당신이 폼으로 보낸 데이터2 : ", data2)
    