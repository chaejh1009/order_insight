# Order Insight

Django와 MySQL로 상품·주문을 관리하고, Apache Spark로 주문 데이터를 처리해 대시보드에 표시하는 실습 프로젝트입니다. CSV·JSONL·로그 읽기부터 조인과 집계, Parquet·Delta Lake 저장, 결과 시각화까지 실습합니다.

## 주요 기능

- 상품 목록 조회 및 주문 생성
- 최근 주문 50건 조회
- 상품 페이지 접속 로그 기록
- 주문 CSV, 상품 JSONL, 접속 로그를 Spark 입력 데이터로 내보내기
- Spark에서 주문 금액·주문일 계산 및 특정 상품 필터링
- 상품 정보와 주문 데이터를 조인해 상품명·카테고리별 주문 수와 매출 집계
- 전체 주문 수·총매출·평균·최대·최소 주문 금액 계산
- 접속 로그를 파싱해 일자별 접속 건수와 처리 시간 합계 계산
- 처리 결과를 JSON 파일로 저장하고 Django 대시보드에서 조회
- 총매출·전체 주문·평균 주문 금액·최대 주문 금액을 요약 카드로 표시
- 분류별 매출 비중 막대, 모바일 반응형 배치, 가로 스크롤 표 제공
- Bronze Parquet 저장 및 기존 Silver Delta 테이블 읽기·덮어쓰기
- 별도 Delta 테이블에서 overwrite·append·UPDATE와 변경 이력 조회 실습
- Python의 `map`·`reduce`, Spark 집계 실행 계획, 입력 파티션 수 비교 실습
- Spark 주문 집계 처리 시간과 입력 파티션을 확인하는 벤치마크 실습

현재 Spark 배치는 주문·상품·접속 로그를 함께 읽고, `product_id` 기준의 broadcast join으로 상품별·카테고리별 집계를 생성합니다. 결과에는 전체 지표, 주문 ID 오름차순 미리보기 최대 10건, 상품별·일자별 매출, 일자별 접속 통계가 포함됩니다. 화면은 실시간 집계가 아니라 마지막으로 저장된 JSON을 표시합니다.

> 현재 `sales_batch.py`는 기존 `data/lake/silver/orders` Delta 테이블을 읽습니다. 새 환경에서 `seed_demo`와 `export_analytics`만 실행하면 이 테이블이 만들어지지 않습니다. 기본 배치를 실행하기 전에 아래의 **현재 배치의 입력과 제약**을 확인하세요.

## 준비 사항

- Python 및 가상환경
- MySQL 서버와 접속 가능한 데이터베이스·계정
- Apache Spark 배포판(`spark-submit`)과 해당 버전이 지원하는 Java·Python 실행 환경
- 기본 매출 배치와 Delta 실습에 사용할 Spark·Scala 버전과 호환되는 Delta Lake 패키지
- `mysqlclient` 설치에 필요한 MySQL 클라이언트 개발 라이브러리와 빌드 도구

Python 의존성은 `requirements.txt`에 정의되어 있습니다. Spark는 별도로 설치하며, 배치는 설치된 배포판의 `spark-submit`으로 실행합니다.

## 설치 및 실행

아래 명령은 프로젝트 루트에서 실행합니다.

### 1. 가상환경과 의존성 설치

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

### 2. 환경 변수 설정

MySQL에 사용할 데이터베이스를 생성하고 계정에 접근 권한을 부여한 뒤, 프로젝트 루트에 `.env` 파일을 작성합니다.

```dotenv
DB_NAME=order_insight
DB_USER=your_mysql_user
DB_PASSWORD=your_mysql_password
DB_HOST=127.0.0.1
DB_PORT=3306
SPARK_SUBMIT=/absolute/path/to/spark/bin/spark-submit
# 아래 두 자리표시자는 설치한 Spark·Scala에 맞는 값으로 바꿉니다.
DELTA_PACKAGE=io.delta:delta-spark_<scala-binary-version>:<delta-version>
```

`SPARK_SUBMIT`은 실제 실행 파일의 절대 경로로 바꿉니다. 현재 설정은 Django 시작 시 이 값을 읽으므로 웹 서버만 실행할 때도 변수를 정의해야 합니다. `DELTA_PACKAGE`는 `--delta` 실행 시 전달하는 Maven 좌표이며, 위 자리표시자를 그대로 사용하면 안 됩니다. `.env`는 Git 추적에서 제외됩니다.

### 3. 데이터베이스와 실습 데이터 준비

```bash
python manage.py migrate
python manage.py seed_demo
```

`seed_demo`는 상품 2개와 주문 12건을 fixture에서 로드하고, 접속 로그 6건과 초기 대시보드 파일을 생성합니다. 다시 실행하면 fixture와 같은 기본 키의 레코드가 덮어써지고 접속 로그·대시보드 파일이 초기화되므로 최초 실습 준비에 사용합니다.

### 4. 웹 서버 실행

```bash
python manage.py runserver
```

| 주소 | 기능 |
| --- | --- |
| http://127.0.0.1:8000/ | 상품 목록으로 이동 |
| http://127.0.0.1:8000/products/ | 상품 조회 및 주문 생성 |
| http://127.0.0.1:8000/orders/ | 최근 주문 조회 |
| http://127.0.0.1:8000/dashboard/ | 마지막 Spark 처리 결과 조회 |

## Spark 배치 실행

가상환경을 활성화한 별도 터미널에서 실행합니다.

```bash
python manage.py export_analytics
python manage.py run_spark_batch --cores 2 --delta
```

기존 Silver Delta 테이블이 준비된 환경에서 실행합니다. 처리가 끝나면 대시보드를 새로고침합니다. 웹에서 생성한 주문을 매출 지표에 반영하려면 내보내기와 배치를 다시 실행해야 하며, 주문 미리보기는 아래 설명처럼 Silver 테이블의 상태에 따라 달라집니다.

데이터는 다음 순서로 이동합니다.

```text
MySQL 상품·주문 + 상품 페이지 접속 로그
  → export_analytics
  → data/raw/orders.csv, products.jsonl, access.log
  → spark_jobs/sales_batch.py
      ├─ CSV 기반 전체·상품별·일자별·분류별 집계
      ├─ data/lake/bronze/orders/에 Parquet 덮어쓰기
      └─ 기존 data/lake/silver/orders/에서 주문 미리보기 생성
  → data/marts/dashboard.json
  → /dashboard/
```

현재 배치는 주문 CSV, 상품 JSONL, 접속 로그를 모두 읽습니다. 상품 JSONL은 콘솔 조회뿐 아니라 주문 데이터와의 조인에도 사용해 상품명·카테고리를 집계 결과에 포함합니다.

`run_spark_batch`는 다음 옵션을 지원합니다.

| 옵션 | 기본값 | 용도 |
| --- | --- | --- |
| `--cores` | `2` | 총 executor 코어 수 지정, `1` 또는 `2` |
| `--script` | `spark_jobs/sales_batch.py` | 실행할 Spark 스크립트 |
| `--data-dir` | 프로젝트의 `data/` | 입력·출력 데이터 루트 |
| `--delta` | 비활성 | Delta 패키지와 SQL 확장·카탈로그 설정 추가 |

실행 후 Spark 제출부터 프로세스 종료까지 걸린 시간을 출력합니다. `export_analytics`도 `--data-dir`를 지원하지만 웹 대시보드는 항상 기본 `data/marts/dashboard.json`을 읽습니다. 접속 로그를 내보낼 때는 기본 `data/raw/access.log`를 원본으로 사용합니다.

`sales_batch.py`는 상품 JSONL을 `product_id`로 주문 CSV와 broadcast join한 뒤 상품별 상품명·카테고리·평균 주문 금액과 카테고리별 매출을 계산합니다. 상품별·카테고리별 집계에서는 상품 데이터에 ID가 존재하지 않는 주문이 inner join에서 제외되므로, 두 데이터의 키가 일치하는지 확인해야 합니다.

관리 명령은 Spark master를 지정하지 않습니다. 로컬 실행을 명시하려면 내보내기 후 직접 제출할 수 있습니다.

```bash
/absolute/path/to/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages "$DELTA_PACKAGE" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  spark_jobs/sales_batch.py \
  --data-dir ./data
```

직접 제출할 때는 `DELTA_PACKAGE`를 셸 환경 변수로도 설정해야 합니다. `.env` 파일은 Django에서 로드하며 `spark-submit`이 자동으로 읽지는 않습니다.

### 현재 배치의 입력과 제약

- 전체 지표(`overall`)와 상품별·일자별·분류별 매출은 `data/raw/orders.csv`를 기준으로 계산합니다.
- 배치 후반에 `orders`를 기존 Silver Delta 테이블로 다시 읽습니다. 최상위 `order_count`와 `preview`는 이 테이블을 기준으로 생성됩니다.
- 따라서 상단의 전체 주문 수와 주문 미리보기의 원본 주문 수는 다를 수 있습니다. 내보내기만으로 Silver 테이블이 갱신되지는 않습니다.
- Bronze Parquet는 CSV에서 덮어쓰지만, 현재 코드에는 Bronze에서 최초 Silver 테이블을 생성하는 단계가 없습니다. Silver 경로에는 `order_id`, `product_id`, `quantity`, `unit_price`, `ordered_at`, `amount`, `order_date` 컬럼을 가진 기존 Delta 테이블이 필요합니다.
- `delta_changes.py`는 `data/demo/delta_orders`를 사용하므로 기본 배치의 Silver 테이블을 준비하거나 갱신하지 않습니다.

새 환경에서 Silver 테이블을 준비하지 않았다면 웹 화면과 Python 실습, 주문 집계 벤치마크, 별도 Delta 실습부터 실행할 수 있습니다.

### 대시보드 구성과 결과 데이터

대시보드는 밝은 배경과 짙은 녹색 포인트로 구성합니다. 상단 요약 카드 아래에 일자별 매출·분류별 비중, 상품별 매출, 주문 미리보기·접속 건수를 배치합니다. 숫자에는 천 단위 구분과 우측 정렬을 적용하며, 좁은 화면에서는 한 열 배치와 표 가로 스크롤을 사용합니다. 빈 집계 목록에는 안내 문구를 표시합니다.

화면은 [dashboard.html](analytics/templates/analytics/dashboard.html)에 구현되어 있으며, 공통 템플릿의 `extra_head` 블록으로 페이지 스타일을 적용합니다. 별도 차트 라이브러리나 JavaScript 없이 HTML·CSS로 표시합니다.

| JSON 항목 | 저장 내용 | 화면 표시 |
| --- | --- | --- |
| `generated_at` | 집계 시각 | 집계 기준 |
| `overall` | CSV 기준 전체 주문 수, 총매출, 평균·최대·최소 주문 금액 | 최소 금액을 제외한 지표 4개를 요약 카드로 표시 |
| `order_count` | Silver 테이블 주문 수 | 주문 미리보기 원본 주문 수 |
| `preview` | 주문 ID, 상품 ID, 수량, 금액 최대 10건 | 주문 미리보기 |
| `by_product` | 상품 ID·상품명·카테고리별 주문 수, 매출, 평균 주문 금액 | 상품별 매출 |
| `by_day` | 주문일별 주문 수, 매출 | 일자별 매출 |
| `page_views` | 접속일별 로그 건수, 처리 시간 합계(`total_duration_ms`) | 접속 건수만 표시 |
| `by_category` | 카테고리별 주문 수, 매출 | 매출·주문 수 및 총매출 대비 비중 막대 |
| `pipeline_seconds` | Spark 세션 준비 후 저장·집계 처리 시간(초) | 하단 처리 시간 및 펼쳐 보는 측정 범위 |

`sales_batch.py`는 주문·상품·접속 로그를 콘솔에 중간 출력한 뒤 `data/marts/dashboard.json`에 결과를 저장합니다. `overall.min_order_amount`와 `page_views.total_duration_ms`는 JSON에만 저장됩니다. 분류별 비중은 `overall.total_revenue`를 분모로 계산하고 정수 퍼센트로 반올림합니다.

`pipeline_seconds`는 Spark 세션 준비 뒤부터 입력 읽기·중간 출력·Parquet/Delta 저장·집계 결과 수신까지 포함하며, 최종 JSON 파일 쓰기와 Spark 종료는 제외합니다. 관리 명령이 출력하는 전체 제출 시간과 측정 범위가 다릅니다.

### Spark 집계 벤치마크

`benchmark_sales.py`는 주문 CSV를 읽고 상품별 주문 수·매출을 집계하면서 읽기·집계·결과 수신 시간과 입력 파티션 수를 출력합니다. 대시보드 JSON은 갱신하지 않습니다.

```bash
python manage.py export_analytics
python manage.py run_spark_batch --script spark_jobs/benchmark_sales.py --cores 2
```

## Delta 변경 이력 실습

```bash
python manage.py export_analytics
python manage.py run_spark_batch --script spark_jobs/delta_changes.py --cores 2 --delta
```

`delta_changes.py`는 주문 ID 12 이하의 데이터를 대상으로 다음 단계를 실행합니다.

1. 2026-09-07 주문을 `data/demo/delta_orders`에 `overwrite`로 저장합니다.
2. 2026-09-08 주문을 `append`로 추가합니다.
3. 주문 ID 1의 수량을 2로 변경하고 금액을 다시 계산합니다.
4. 상품별 매출과 `DESCRIBE HISTORY`의 버전·작업·파라미터를 출력합니다.

실행할 때마다 데모 테이블의 현재 데이터를 첫 단계에서 덮어씁니다. 해당 날짜의 주문이 CSV에 있어야 예시 결과를 관찰할 수 있습니다. 대시보드 JSON과 기본 Silver 테이블은 갱신하지 않습니다.

## Map/Reduce와 부분 집계 실습

### Python으로 집계 과정 확인

다음 스크립트는 Python 표준 라이브러리만 사용하며 MySQL이나 Spark 없이 실행할 수 있습니다.

```bash
python serialize/raw_python.py
python serialize/parallel_process.py
```

- `raw_python.py`: 주문 12건을 두 묶음으로 나누어 `map`·`reduce`로 부분 집계한 뒤, 결과를 다시 합칩니다. 최종 결과는 A 상품 주문 5건·매출 1,500, B 상품 주문 7건·매출 2,800입니다. 실행은 단일 Python 프로세스에서 순차적으로 이루어집니다.
- `parallel_process.py`: 입력 1,000만 행, 상품 100개, 파티션 8개라는 가정으로 부분 집계 결과의 최대 행 수(800)와 병렬 처리 예상 시간(25초)을 계산합니다. 실제 병렬 처리나 성능 측정 코드는 아닙니다.

### Spark로 같은 데이터 집계

```bash
python manage.py run_spark_batch --script spark_jobs/map_reduce_demo.py --cores 2
```

`map_reduce_demo.py`는 코드에 정의된 주문 12건을 2개 파티션으로 구성하고, 상품별 주문 수·매출·평균 주문 금액과 실행 계획을 출력합니다. 평균 주문 금액은 A 상품 300, B 상품 400입니다. 관리 명령이 전달하는 `--data-dir` 인자는 받지만 파일을 읽거나 대시보드를 갱신하지 않습니다.

로컬 실행을 명시하려면 다음과 같이 직접 제출합니다.

```bash
/absolute/path/to/spark/bin/spark-submit \
  --master 'local[2]' \
  spark_jobs/map_reduce_demo.py \
  --data-dir ./data
```

## 프로젝트 구조

```text
order_insight/       Django 설정, 루트 URL, 공통 템플릿
shop/                상품·주문 모델, 화면, 실습 fixture
analytics/           대시보드 화면 및 데이터 관리 명령
spark_jobs/          매출 배치, 벤치마크, Map/Reduce 및 Delta 변경 실습
serialize/           Python 부분 집계와 병렬 처리 가정 계산 실습
data/raw/            Spark 입력 CSV·JSONL 및 접속 로그
data/marts/          대시보드용 처리 결과 JSON
data/lake/bronze/    원본 주문 Parquet 저장 경로
data/lake/silver/    기본 배치가 읽는 기존 주문 Delta 테이블
data/demo/           별도 Delta 변경 이력 실습 데이터
manage.py            Django 관리 명령 진입점
requirements.txt    Python 의존성
```

## 설정 확인

```bash
python manage.py check
python manage.py test
```

현재 `shop/tests.py`와 `analytics/tests.py`에는 구현된 테스트가 없습니다. 설정은 `DEBUG=True` 등 로컬 실습용으로 구성되어 있습니다.

## 실행 문제 확인

| 증상 | 확인할 내용 |
| --- | --- |
| Django 시작 시 `SPARK_SUBMIT` 관련 오류 | `.env`에 실행 파일 경로가 정의되어 있는지 확인 |
| MySQL 연결 실패 | MySQL 실행 여부, 데이터베이스·계정, `.env`의 접속 정보 확인 |
| 상품 페이지에서 접속 로그 경로 오류 | 최초 실행 시 `seed_demo`로 `data/raw/access.log` 준비 |
| Delta 데이터 소스를 찾지 못함 | `--delta` 옵션과 호환되는 `DELTA_PACKAGE` 설정 확인 |
| Silver 경로가 없거나 Delta 테이블이 아니라는 오류 | `data/lake/silver/orders`의 기존 Delta 테이블 준비 여부 확인 |
| 새 주문이 대시보드에 보이지 않음 | 내보내기·배치 완료 여부 확인. 미리보기는 별도로 Silver 데이터 상태 확인 |
| 대시보드 JSON 파일 오류 | `data/marts/dashboard.json` 존재 여부와 JSON 형식 확인. 초기화가 목적일 때만 `seed_demo` 재실행 |
