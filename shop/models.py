from django.db import models


class Product(models.Model):
    id = models.CharField(max_length=20, primary_key=True)
    name = models.CharField(max_length=100)
    category = models.CharField(max_length=50)
    unit_price = models.PositiveIntegerField()

    def __str__(self):
        return self.name


class Order(models.Model):
    product = models.ForeignKey(Product, on_delete=models.PROTECT)
    quantity = models.PositiveIntegerField()
    unit_price = models.PositiveIntegerField()
    ordered_at = models.DateTimeField()

    # getter -> 가공된 지표나, 조회하는 값이 잘못되지 않도록 도와주는 메서드
    @property
    def amount(self):
        """quantity * unit_price를 리턴해, 해당 주문의 총 매출액을 합산해줍니다."""
        return self.quantity * self.unit_price