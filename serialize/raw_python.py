from functools import reduce

rows = [
    ("A", 100), ("A", 200), ("A", 300),
    ("B", 100), ("B", 200), ("B", 300),
    ("A", 400), ("A", 500),
    ("B", 400), ("B", 500), ("B", 600), ("B", 700),
]

print("전체 데이터 개수 : ", len(rows))
print("원본 row[0] : ", rows[0])

totals = {}
# rows가 [0], [1]에 해당하는 자료를 가진 자료형이므로, 0번째는 product_id에, 1번째는 amount변수에 쪼개서 할당 후 반복문 동작.
for product_id, amount in rows:
    old_count, old_revenue = totals.get(product_id, (0, 0))
    totals[product_id] = (old_count + 1, old_revenue + amount)

print("딕셔너리에 집계한 totals : ", totals)

def to_pair(order):
    product_id, amount = order
    # return시 2개 이상의 변수를 나열하면, 튜플(1번째값, 2번쨰값....) 형식으로 리턴
    return (product_id, (1, amount))

print("row[0]을 정제한 결과물 : ",to_pair(rows[0]))

# totals는 누적해서 저장할 저장소, pair는 개별 주문건수
def add_pair(totals, pair):
    print("전체 데이터 : ",totals)
    product_id, (count, revenue) = pair
    # 직전까지 누적된 데이터값 조회 후
    old_count, old_revenue = totals.get(product_id, (0, 0))
    # 직전 누적값에 새로운 데이터의 값을 합산한 다음 갱신해서 저장
    totals[product_id] = (old_count + count, old_revenue + revenue)
    # 그리고 갱신된 전체 데이터를 리턴
    return totals

print("빈 totals에 정제후 row[0]을 집어넣은 모습 : ", add_pair({}, ("A", (1, 100))))

# 리듀스 연산을 통해, map을 통해 나온 결과물을 최종적으로 합산해줍니다.
print("map연산으로 0번째 - 5번째 자료 정제:", list(map(to_pair, rows[:6])))
print("map연산으로 6번째 - 마지막 자료 정제:", list(map(to_pair, rows[6:])))
partial_p0 = reduce(add_pair, map(to_pair, rows[:6]), {})
partial_p1 = reduce(add_pair, map(to_pair, rows[6:]), {})
print("map연산으로 정제된 자료를 집계한 reduce 결과물 0 : ", partial_p0)
print("map연산으로 정제된 자료를 집계한 reduce 결과물 1 : ", partial_p1)

# 병렬처리된 부분적인 결과물 최종 합산해 집계하는 코드
print("partial_p0 최종 집계 전 : ", list(partial_p0.items()))
print("partial_p1 최종 집계 전 : ", list(partial_p1.items()))
partial_rows = list(partial_p0.items()) + list(partial_p1.items())
final_totals = reduce(add_pair, partial_rows, {})

print(partial_rows)
print(final_totals)