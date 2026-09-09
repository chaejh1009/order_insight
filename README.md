# Order Insight

Django와 MySQL로 상품·주문을 관리하고, Apache Spark로 주문 데이터를 처리해 대시보드에 표시하는 실습 프로젝트입니다.

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
- Python의 `map`·`reduce`, Spark 집계 실행 계획, 입력 파티션 수 비교 실습
- Spark 주문 집계 처리 시간과 입력 파티션을 확인하는 벤치마크 실습

현재 Spark 배치는 주문·상품·접속 로그를 함께 읽고, `product_id` 기준의 broadcast join으로 상품별·카테고리별 집계를 생성합니다. 결과에는 전체 지표, 주문 ID 오름차순 미리보기 최대 10건, 상품별·일자별 매출, 일자별 접속 통계가 포함됩니다.

## 준비 사항

- Python 및 가상환경
- MySQL 서버와 접속 가능한 데이터베이스·계정
- Apache Spark 배포판(`spark-submit`)과 해당 버전이 지원하는 Java·Python 실행 환경
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
```

`SPARK_SUBMIT`은 실제 실행 파일의 절대 경로로 바꿉니다. 현재 설정은 Django 시작 시 이 값을 읽으므로 웹 서버만 실행할 때도 변수를 정의해야 합니다. `.env`는 Git 추적에서 제외됩니다.

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
python manage.py run_spark_batch --cores 2
```

처리가 끝나면 대시보드를 새로고침합니다. 웹에서 생성한 주문을 대시보드에 반영하려면 내보내기와 배치를 다시 실행해야 합니다.

데이터는 다음 순서로 이동합니다.

```text
MySQL 상품·주문 + 상품 페이지 접속 로그
  → export_analytics
  → data/raw/orders.csv, products.jsonl, access.log
  → spark_jobs/sales_batch.py
  → data/marts/dashboard.json
  → /dashboard/
```

현재 배치는 주문 CSV, 상품 JSONL, 접속 로그를 모두 읽습니다. 상품 JSONL은 콘솔 조회뿐 아니라 주문 데이터와의 조인에도 사용해 상품명·카테고리를 집계 결과에 포함합니다.

`run_spark_batch`는 `--cores 1` 또는 `--cores 2`를 지원하고, `--script`로 실행할 스크립트를 지정할 수 있습니다. 실행 후 Spark 제출부터 프로세스 종료까지 걸린 시간을 출력합니다. 두 명령 모두 `--data-dir`로 데이터 경로를 바꿀 수 있지만, 웹 대시보드는 기본 `data/marts/dashboard.json`을 읽습니다. 접속 로그를 내보낼 때는 기본 `data/raw/access.log`를 원본으로 사용합니다.

`sales_batch.py`는 상품 JSONL을 `product_id`로 주문 CSV와 broadcast join한 뒤 상품별 상품명·카테고리·평균 주문 금액과 카테고리별 매출을 계산합니다. 상품별·카테고리별 집계에서는 상품 데이터에 ID가 존재하지 않는 주문이 inner join에서 제외되므로, 두 데이터의 키가 일치하는지 확인해야 합니다.

관리 명령은 Spark master를 지정하지 않습니다. 로컬 실행을 명시하려면 내보내기 후 직접 제출할 수 있습니다.

```bash
/absolute/path/to/spark/bin/spark-submit \
  --master 'local[2]' \
  spark_jobs/sales_batch.py \
  --data-dir ./data
```

### 대시보드 결과와 현재 구현 범위

| JSON 항목 | 저장 내용 | 화면 표시 |
| --- | --- | --- |
| `generated_at` | 집계 시각 | 집계 기준 |
| `overall` | 전체 주문 수, 총매출, 평균·최대·최소 주문 금액 | 현재 미표시 |
| `order_count` | 전체 주문 수 | 주문 수 |
| `preview` | 주문 ID, 상품 ID, 수량, 금액 최대 10건 | 주문 미리보기 |
| `by_product` | 상품 ID·상품명·카테고리별 주문 수, 매출, 평균 주문 금액 | 상품별 매출 |
| `by_day` | 주문일별 주문 수, 매출 | 일자별 매출 |
| `page_views` | 접속일별 로그 건수, 처리 시간 합계(`total_duration_ms`) | 접속 건수만 표시 |
| `by_category` | 카테고리별 주문 수, 매출 | 분류별 매출 |

`sales_batch.py`는 주문·상품·접속 로그를 콘솔에 중간 출력한 뒤 `data/marts/dashboard.json`에 결과를 저장합니다. 상품별 평균 주문 금액은 JSON에 저장되고 대시보드에도 표시됩니다. `overall`의 세부 지표와 `page_views.total_duration_ms`는 현재 JSON에만 저장됩니다.

### Spark 집계 벤치마크

`benchmark_sales.py`는 주문 CSV를 읽고 상품별 주문 수·매출을 집계하면서 읽기·집계·결과 수신 시간과 입력 파티션 수를 출력합니다. 대시보드 JSON은 갱신하지 않습니다.

```bash
python manage.py export_analytics
python manage.py run_spark_batch --script spark_jobs/benchmark_sales.py --cores 2
```

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
spark_jobs/          매출·접속 로그 배치 및 Map/Reduce 집계 실습
                     (benchmark_sales.py는 주문 집계 성능 관찰용)
serialize/           Python 부분 집계와 병렬 처리 가정 계산 실습
data/raw/            Spark 입력 CSV·JSONL 및 접속 로그
data/marts/          대시보드용 처리 결과 JSON
manage.py            Django 관리 명령 진입점
requirements.txt    Python 의존성
```

## 설정 확인

```bash
python manage.py check
python manage.py test
```

현재 `shop/tests.py`와 `analytics/tests.py`에는 구현된 테스트가 없습니다. 설정은 `DEBUG=True` 등 로컬 실습용으로 구성되어 있습니다.
