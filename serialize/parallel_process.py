input_rows = 10_000_000
product_count = 100
input_partitions = 8

partial_rows_upper_bound = product_count * input_partitions
print("읽어야 하는 원문 행:", input_rows)
print("가정한 부분 결과의 최대 행:", partial_rows_upper_bound)

serial_seconds_assumption = 40
parallel_workers_assumption = 2
extra_seconds_assumption = 5
parallel_seconds_assumption = (
    serial_seconds_assumption / parallel_workers_assumption + extra_seconds_assumption
)

print("가정한 경과 시간:", parallel_seconds_assumption)

