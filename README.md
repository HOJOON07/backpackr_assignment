# 백패커 Data Engineer 과제

## 1. 과제 개요

Kaggle ecommerce behavior 로그 데이터(`2019-Oct.csv`, `2019-Nov.csv`)를 Hive External Table로 제공하기 위한 Spark Application입니다.

주요 처리 내용은 다음과 같습니다.

- UTC 기준 `event_time`을 KST 기준으로 변환하여 daily partition 생성
- 동일 `user_id` 내 이벤트 간격이 5분 이상인 경우 새로운 `session_id` 생성
- 처리 결과를 Parquet + Snappy 형식으로 저장
- 저장된 데이터를 Hive External Table로 등록
- Hive External Table을 사용해 `user_id` 기준 WAU와 `session_id` 기준 WAU 계산

## 2. 요구사항 대응표

| 요구사항 | 구현 내용 |
|----------|-----------|
| Spark Application 작성 | Java 기반 Spark Application으로 원본 CSV를 처리하고 Hive External Table을 생성했습니다. |
| KST 기준 daily partition | UTC 기준 `event_time`을 KST로 변환해 `event_date_kst` 파티션을 생성했습니다. |
| 5분 이상 간격 기준 세션 생성 | 동일 `user_id` 내 이벤트 간격이 5분 이상이면 새로운 `session_id`를 생성했습니다. |
| Parquet + Snappy 저장 | 처리 결과를 Parquet 형식으로 저장하고 Snappy 압축을 적용했습니다. |
| External Table 설계 | 저장된 Parquet 경로를 Hive External Table의 `LOCATION`으로 등록했습니다. |
| 추가 기간 처리 | 처리 기간을 파라미터로 받고, Bronze를 UTC 날짜 파티션으로 두어 필요한 범위만 읽도록 구현했습니다. |
| 재처리 및 장애 복구 | Dynamic Partition Overwrite와 audit manifest를 사용해 재처리와 실행 이력 추적을 기록하도록 구성했습니다. |
| WAU 계산 | Hive External Table을 사용해 `user_id` 기준 WAU와 `session_id` 기준 WAU를 계산했습니다. |
| 언어 선택 사유 | Java를 선택한 이유와 Spark SQL 중심 설계 방향을 문서화했습니다. |

## 3. 기술 스택 및 실행 환경

- Java 17
- Maven
- Apache Spark 4.1.1
- Docker Compose
- Parquet + Snappy
- Hive External Table
- Spark local metastore


## 4. 프로젝트 구조

```
.
├── docker-compose.yml / Dockerfile / docker/run.sh
├── conf/                       # 실행 설정 (local, prod 예시)
├── data/
│   ├── raw/                    # 원본 CSV (직접 배치)
│   ├── warehouse/              # Bronze / Silver / Gold 저장 위치
│   └── manifests/batch_runs/   # 배치 실행 기록
└── src/main/
    ├── java/com/backpackr/de/
    │   ├── App.java            # 진입점, CLI 파싱, 모드 분기
    │   ├── config/             # 설정 로딩
    │   ├── io/                 # CSV 읽기, 파티션 write
    │   ├── job/                # Bronze / Ingestion / Wau / BuildWau
    │   ├── meta/               # External Table DDL, audit manifest
    │   └── util/               # SparkSession, SQL 로더
    └── resources/sql/          # 세션화 / 테이블 DDL / WAU 쿼리
```

## 5. 실행 방법

### 5.1 원본 데이터 준비

Kaggle에서 제공하는 `2019-Oct.csv`, `2019-Nov.csv` 파일을 다운로드한 뒤 아래 경로에 저장합니다.

```text
data/raw/
├── 2019-Oct.csv
└── 2019-Nov.csv
```

### 5.2 Docker 이미지 빌드

```bash
docker compose build
```

### 5.3 데이터 처리 실행

처리 단계를 명확히 확인할 수 있도록 `build-bronze`, `ingest`, `build-wau`, `wau` 모드로 실행합니다.

**1. CSV → Bronze** — 원본 CSV를 읽어 UTC 날짜 기준 Parquet 데이터로 저장합니다.

```bash
docker compose run --rm spark-job \
  --mode build-bronze \
  --config /opt/app/conf/local.properties
```

**2. Bronze → Silver** — Bronze 데이터를 읽어 KST 기준 daily partition을 생성하고, `user_id`별 5분 간격 기준으로 `session_id`를 생성합니다.

```bash
docker compose run --rm spark-job \
  --mode ingest \
  --config /opt/app/conf/local.properties \
  --start-date 2019-10-01 \
  --end-date 2019-11-30
```

**3. Silver → Gold** — 세션화된 Silver 데이터를 기반으로 WAU 집계 결과를 생성합니다.

```bash
docker compose run --rm spark-job \
  --mode build-wau \
  --config /opt/app/conf/local.properties
```

**4. WAU 조회** — Hive External Table을 기준으로 `user_id`, `session_id` 기준 WAU를 조회합니다.

```bash
docker compose run --rm spark-job \
  --mode wau \
  --config /opt/app/conf/local.properties
```

### 5.4 실행 결과 확인

처리 결과는 아래 경로에 생성됩니다.

```text
data/warehouse/
├── activity_events_bronze/     # Bronze (UTC 파티션)
├── ecommerce_activity_events/  # Silver (KST 파티션, 세션화)
└── weekly_active_users/        # Gold (WAU 집계)
```

배치 실행 기록은 아래 경로에 생성됩니다.

```text
data/manifests/batch_runs/
```

### 5.5 실행 환경 참고

전체 데이터는 약 1억 행 이상이므로, Docker Desktop에 충분한 메모리를 할당하는 것을 권장합니다.
아래 환경에서 실행을 확인했습니다.

- MacBook Pro, 48GB RAM
- Docker Desktop memory: 24GB
- Spark driver memory: 16GB

## 6. 원본 데이터 확인

원본 데이터는 `event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session` 9개 컬럼으로 구성되어 있습니다.

구현 전 원본 데이터를 확인하고 다음과 같이 처리 기준을 정했습니다.

| 항목 | 처리 기준 |
|---|---|
| `event_time` | `2019-10-01 00:00:00 UTC` 형식의 UTC 문자열로 확인했습니다. |
| CSV schema | 모든 컬럼을 문자열로 읽고 Spark SQL 단계에서 명시적으로 타입 변환했습니다. |
| `event_type` | `view`, `cart`, `purchase` 세 가지 값으로 확인했습니다. |
| `category_code`, `brand` | 값이 비어 있는 경우가 있어, 임의의 값으로 채우지 않고 그대로 유지했습니다.  |
| `user_session` | 원본 값은 보존하되, 과제 기준에 따라 새로운 `session_id`를 생성했습니다. |

## 7. 주요 설계 결정

**언어 — Java.**  
이번 과제에서는 Spark Application의 실행 흐름을 명확하게 구성하고, 설정 관리·입출력 제어·Hive table 관리·manifest 기록 같은 배치 애플리케이션의 실행 관련 로직을 안정적으로 작성하기 위해 Java를 선택했습니다.

Spark에서 Scala는 더 간결한 표현이 가능하지만, 이번 구현에서는 핵심 데이터 변환 로직을 Spark SQL로 분리했습니다. 따라서 언어별 API 표현의 차이보다, timestamp 변환, sessionization, WAU 계산 로직이 SQL 단위로 명확히 드러나는 구조를 우선했습니다.

Java는 애플리케이션의 실행 흐름을 담당하고, Spark SQL은 데이터 처리 로직을 담당하도록 역할을 나누어 구현했습니다. 이를 통해 각 단계의 책임을 분리하고, 처리 로직을 검증·수정하기 쉬운 구조로 구성했습니다.

**메타스토어 — Spark local metastore.**  
로컬 환경에서 별도 Hive Metastore 서버를 구성하지 않고도 실행 결과를 확인할 수 있도록 Spark local metastore를 사용했습니다.

이번 구현에서 중요한 점은 metastore의 종류보다, Spark가 생성한 Parquet 데이터를 Hive External Table의 `LOCATION`으로 등록하는 방식입니다. 따라서 로컬에서는 Spark local metastore를 사용하되, 테이블은 Managed Table이 아닌 External Table로 생성했습니다.

운영 환경에서는 여러 Spark Job이나 쿼리 엔진이 동일한 테이블 메타데이터를 공유해야 하므로 Remote Hive Metastore 또는 AWS Glue Data Catalog와 같은 shared metastore를 사용하는 것이 더 적절합니다. 이를 고려해 metastore mode, warehouse path, table location, database/table name은 설정으로 분리했습니다.

**레이어 분리 — Bronze / Silver / Gold.**  
CSV를 바로 세션화하여 최종 테이블을 만들 수도 있었지만, 추가 기간 처리와 재처리를 고려해 Bronze, Silver, Gold 단계를 분리했습니다.

Bronze는 원본 CSV를 UTC 날짜 기준 Parquet으로 저장하는 단계입니다. 원본 CSV는 파일 단위 데이터이기 때문에 처리 기간에 필요한 데이터만 효율적으로 읽기 어렵습니다. 따라서 Bronze를 날짜 파티션으로 저장해 이후 재처리나 추가 기간 처리 시 필요한 범위만 읽을 수 있도록 했습니다.

Silver는 KST 기준 daily partition과 새로 생성한 `session_id`가 포함된 activity event 테이블입니다. Gold는 Silver 테이블을 기반으로 계산한 WAU 집계 결과입니다.

단계를 불필요하게 늘리기보다는, 원본 보존, 세션화 결과 제공, WAU 집계라는 목적이 명확한 단위로만 분리했습니다.

**session_id 형식.**  
`session_id` 생성 방식으로 해시 기반 ID도 검토했습니다. 해시 방식은 고정 길이와 균등한 분포라는 장점이 있지만, 결과 검증과 디버깅이 어렵다는 단점이 있습니다.

이번 구현에서는 사람이 직접 추적하기 쉬운 `{user_id}_{세션 시작 UTC epoch}` 형식을 사용했습니다. 이 방식은 동일 입력을 재처리해도 같은 값이 생성되며, 특정 사용자의 세션을 확인하기 쉽습니다.

또한 원본 데이터의 `user_session`은 별도 컬럼으로 보존하고, 과제 요구사항에 따라 새로 생성한 `session_id`와 구분했습니다.

**Gold 갱신 — 풀리프레시.**  
WAU는 주 단위 distinct 집계입니다. 정확한 WAU를 계산하려면 해당 주의 전체 이벤트를 함께 봐야 하므로, 이번 구현에서는 Gold를 풀리프레시 방식으로 계산했습니다.

이 방식은 구현이 단순하고 결과 검증이 명확하다는 장점이 있습니다. 다만 운영 환경에서 데이터 기간이 길어지고 매일 갱신해야 한다면, 매번 전체 기간을 다시 계산하는 비용이 커질 수 있습니다.

이 경우 Gold를 주 단위로 파티션하고, 새 데이터로 인해 영향을 받는 주만 해당 주 전체를 다시 집계해 교체하는 방식으로 확장하는 것이 적절하다고 판단했습니다.

**구현 중 해결한 문제**

- **로컬 메타스토어 경로 충돌**  
  Spark local metastore가 `metastore_db`를 생성하는 과정에서 Docker volume에 의해 동일 경로가 미리 생성되어 충돌하는 문제가 있었습니다. 이를 해결하기 위해 부모 디렉토리를 마운트하고, metastore DB 경로는 connection URL에서 명시하도록 조정했습니다.

- **Jackson 버전 충돌**  
  manifest 직렬화를 위해 추가한 Jackson 의존성이 Spark 런타임에서 사용하는 Jackson 버전과 충돌하는 문제가 있었습니다. 별도 Jackson 의존성을 제거하고, Spark 런타임에서 제공하는 버전을 사용하도록 정리했습니다.

- **작은 파일 문제**  
  세션화 과정에서 shuffle이 발생하면서 날짜별로 작은 Parquet 파일이 많이 생성되는 문제가 있었습니다. 이를 완화하기 위해 저장 전에 `event_date_kst` 기준으로 repartition하고, 파일당 row 수 상한을 설정했습니다.

## 8. 데이터 처리 흐름

```text
CSV(UTC) ─build-bronze─▶ Bronze(UTC 파티션) ─ingest─▶ Silver(KST 파티션, 세션화) ─build-wau─▶ Gold(WAU)
```

**KST daily partition.**
원본 event_time은 `2019-10-01 00:00:00 UTC` 형식의 UTC 문자열입니다. 한국 기준 daily partition을 생성하기 위해 UTC suffix를 제거한 뒤 `event_time_utc`로 파싱하고, `from_utc_timestamp(event_time_utc, 'Asia/Seoul')`를 사용해 `event_time_kst`로 변환했습니다.

이후 `event_time_kst`에서 날짜를 추출해 `event_date_kst`를 partition key로 사용했습니다. 예를 들어 `2019-10-25 15:16:00 UTC`는 KST 기준 `2019-10-26 00:16:00`이므로 `event_date_kst=2019-10-26` 파티션에 저장됩니다.

**세션화.**
동일 `user_id` 내 이벤트를 `event_time_utc` 기준으로 정렬한 뒤, window lag를 사용해 직전 이벤트 시각을 가져왔습니다. 현재 이벤트와 직전 이벤트의 간격이 5분 이상이면 새로운 세션으로 판단하고, 새 세션 여부를 누적합하여 사용자별 세션 번호를 생성했습니다. 각 세션의 시작 시각(UTC epoch)을 사용해 `{user_id}_{epoch}` 형식의 `session_id`를 만들었습니다.

세션 간격 계산과 `session_id` 생성을 위한 epoch 계산은 UTC 기준으로 했습니다. KST는 daily partition과 사람이 확인하기 위한 시각 표현에 사용하고, 세션의 시간 간격과 epoch 계산은 원본 이벤트 시점인 UTC 기준으로 처리했습니다.

**증분 처리.**
원본 CSV는 날짜 파티션이 없기 때문에 처리 기간을 지정하더라도 필요한 파일 일부만 효율적으로 읽기 어렵습니다. 이를 보완하기 위해 먼저 Bronze 단계에서 원본 데이터를 UTC 날짜 기준 Parquet으로 저장했습니다.

이후 ingest 단계에서는 KST 대상 기간을 UTC 범위로 변환하고, 세션 경계 확인에 필요한 버퍼 기간을 포함해 필요한 Bronze 파티션만 읽도록 구성했습니다. 예를 들어 11월 데이터를 처리할 때 전체 Bronze를 다시 읽지 않고, 세션 경계에 필요한 이전 구간을 포함한 UTC 날짜 파티션만 읽을 수 있습니다. 해당 동작은 Spark 실행 계획의 `PartitionFilters`를 통해 확인했습니다.

저장 결과는 Parquet + Snappy 형식으로 생성했습니다. 또한 세션화 과정에서 shuffle이 발생하므로, write 전에 `event_date_kst` 기준으로 repartition하고 파일당 row 수 상한을 설정해 작은 파일이 과도하게 생성되지 않도록 조정했습니다.

## 9. Hive External Table 설계

```sql
CREATE EXTERNAL TABLE ecommerce.activity_events (
    event_time_utc      TIMESTAMP,
    event_time_kst      TIMESTAMP,
    event_type          STRING,
    product_id          BIGINT,
    category_id         BIGINT,
    category_code       STRING,
    brand               STRING,
    price               DOUBLE,
    user_id             BIGINT,
    source_user_session STRING,
    session_id          STRING,
    session_start_utc   TIMESTAMP
)
PARTITIONED BY (event_date_kst DATE)
STORED AS PARQUET
LOCATION '${output_path}'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');
```

Spark가 생성한 Parquet 데이터를 Hive External Table로 등록했습니다. External Table을 사용하면 테이블 메타데이터와 실제 데이터 파일의 위치를 분리할 수 있습니다. 테이블을 삭제하거나 다시 생성하더라도 `LOCATION` 경로의 데이터 파일은 유지됩니다.

`LOCATION`은 설정값으로 분리했습니다. 로컬 환경에서는 `file://` 경로를 사용하고, 운영 환경에서는 동일한 코드 구조에서 `s3a://` 또는 HDFS 경로로 전환할 수 있도록 했습니다.

추가 기간을 적재한 뒤에는 `MSCK REPAIR TABLE`을 실행해 새로 생성된 partition metadata를 Hive metastore에 반영했습니다.

## 10. 재처리 및 장애 복구 전략

재처리 시 단순 append를 사용하면 같은 기간을 다시 처리할 때 중복 파일이 누적될 수 있습니다. 이를 방지하기 위해 dynamic partition overwrite를 사용했습니다.

dynamic partition overwrite를 사용하면 이번 배치 결과에 포함된 partition만 overwrite 대상이 됩니다. 따라서 동일 기간을 재처리할 때 append로 인한 중복 파일 생성을 방지하고, 대상이 아닌 기존 partition은 유지할 수 있습니다.

다만 일반 Parquet 기반 External Table은 Delta Lake나 Iceberg 같은 트랜잭션 테이블 포맷이 아니기 때문에 완전한 atomic transaction을 보장하지는 않습니다. 이번 구현에서는 동일 입력과 동일 기간을 재실행했을 때 결과가 중복 누적되지 않도록 멱등성을 확보하는 데 중점을 두었습니다.

`session_id`는 `run_id`, 실행 시점, 랜덤값에 의존하지 않고 `user_id`와 세션 시작 시각이라는 입력 데이터의 결정적인 값으로 생성했습니다. 따라서 동일 입력을 재처리하면 동일한 `session_id`가 생성됩니다.

또한 실행할 때마다 `data/manifests/batch_runs/` 경로에 `run_id`별 manifest를 기록했습니다. manifest에는 실행 상태, 처리 기간, row count, 시작/종료 시각, 실패 시 에러 메시지를 남깁니다. 실패한 경우 manifest에 기록된 처리 기간과 입력 경로를 기준으로 동일 파라미터를 다시 실행할 수 있도록 했습니다.

자동 재시도나 스케줄링은 Airflow와 같은 오케스트레이터에서 담당하는 것이 적절하다고 보고, Spark Application 내부에서는 재처리 가능한 상태 기록과 멱등 실행에 집중했습니다.

## 11. WAU 계산 쿼리 및 결과

WAU는 KST 기준 `event_date_kst`를 사용해 월요일 시작 주 단위로 계산했습니다.

**user_id 기준 WAU**

```sql
SELECT
    date_trunc('week', event_date_kst) AS week_start_date,
    count(DISTINCT user_id) AS wau_by_user
FROM ecommerce.activity_events
GROUP BY date_trunc('week', event_date_kst)
ORDER BY week_start_date;
```

**session_id 기준 WAU**

```sql
SELECT
    date_trunc('week', event_date_kst) AS week_start_date,
    count(DISTINCT session_id) AS wau_by_session
FROM ecommerce.activity_events
GROUP BY date_trunc('week', event_date_kst)
ORDER BY week_start_date;
```

`date_trunc('week', ...)`는 timestamp를 반환하므로 `week_start_date`에는 자정(`00:00:00`)이 포함됩니다. 아래 결과 표에서는 가독성을 위해 날짜만 표기했습니다.

`user_id` 기준 WAU는 주간 활성 사용자 수입니다. `session_id` 기준 WAU는 같은 사용자가 한 주에 여러 세션을 가질 수 있으므로, 엄밀히는 주간 활성 세션 수에 가까운 지표입니다.

| week_start_date | wau_by_user | wau_by_session |
|---|---|---|
| 2019-09-30 | 818,388 | 1,570,536 |
| 2019-10-07 | 1,057,958 | 2,154,180 |
| 2019-10-14 | 1,090,898 | 2,257,214 |
| 2019-10-21 | 1,093,146 | 2,153,837 |
| 2019-10-28 | 1,054,722 | 2,115,233 |
| 2019-11-04 | 1,321,141 | 2,751,842 |
| 2019-11-11 | 1,543,309 | 4,754,423 |
| 2019-11-18 | 1,376,755 | 2,876,494 |
| 2019-11-25 | 1,133,949 | 2,265,384 |

11월 둘째 주(2019-11-11)의 WAU가 가장 높게 나타났습니다. 이번 과제에서는 외부 이벤트 원인 분석보다, Hive External Table 기반 WAU 계산 결과와 교차 검증에 초점을 두었습니다.

계산 결과는 DuckDB로 Parquet 데이터를 직접 읽어 다시 계산했고, Spark에서 산출한 결과와 일치하는 것을 확인했습니다.

**Partial Week 해석**

첫 주(2019-09-30)와 마지막 주(2019-11-25)는 7일 전체가 포함되지 않은 partial week입니다.

WAU는 KST 기준 주간 지표이지만, 제공된 원본 데이터는 UTC 기준 2019년 10월과 11월 범위입니다. 따라서 제공 데이터의 시작과 끝이 KST 기준 주간 경계를 완전히 커버하지 않습니다.

첫 주는 KST 기준 2019-09-30 ~ 2019-10-06 주차이지만, 원본 데이터에는 9월 30일 데이터가 포함되어 있지 않습니다. 마지막 주는 KST 기준 2019-11-25 ~ 2019-12-01 주차이지만, 제공된 원본 데이터가 해당 KST 주간 전체를 커버하지 않습니다.

따라서 양끝 주의 WAU는 가운데 주차와 직접 비교하기보다는, 데이터 제공 범위에 따른 해석상의 제약이 있는 값으로 보는 것이 적절합니다.

## 12. 로컬/운영 환경 전환 고려

`spark.master`, `metastore.mode`, `input/output path`, `warehouse path`, `database/table name`은 설정값으로 분리했습니다. 로컬 환경에서는 `file://` 기반 경로와 Spark local metastore를 사용하고, 운영 환경에서는 `s3a://` 또는 HDFS 경로와 Remote Hive Metastore 또는 AWS Glue Data Catalog로 전환할 수 있도록 고려했습니다.

데이터 규모가 커지면 `executor memory`, `executor cores`, `spark.sql.shuffle.partitions` 등을 조정하고 클러스터 환경에서 실행하는 방식으로 확장할 수 있습니다. 운영 환경 설정 예시는 `conf/prod.properties.example`에 분리했습니다.

Gold 갱신은 현재 풀리프레시 방식으로 구현했습니다. 데이터가 수년치로 커지고 매일 갱신해야 한다면, 바뀌지 않은 과거 주까지 매번 다시 계산하는 비용이 커질 수 있습니다. 이 경우 Gold를 주 단위로 파티션하고, 새 데이터로 인해 영향을 받는 주만 해당 주 전체를 재집계해 교체하는 방식으로 전환하는 것이 적절하다고 판단했습니다.

## 13. AI 도구 사용 내역

Claude Code를 사용했습니다.

- **설계 논의:** 언어, 빌드 도구, 메타스토어 선택, 레이어 분리 여부, 증분 처리 전략을 트레이드오프 중심으로 검토했습니다.
- **구현과 디버깅:** Java 코드, Spark SQL, Docker/Maven 설정의 초안 작성과 실행 중 발생한 문제 해결 과정에서 보조적으로 활용했습니다.
- **검증:** DuckDB 교차 검증, 단위 테스트, 증분 파티션 프루닝 실행 계획 확인 과정에서 보조적으로 활용했습니다.

AI 도구의 제안을 그대로 적용하기보다, 데이터 크기, 제출 기한, Hive External Table 특성, 로컬 재현성 같은 제약을 함께 고려해 대안을 비교하는 용도로 사용했습니다.

기술 선택의 최종 판단, 요구사항 해석, 5분 세션 경계 기준, WAU 주 정의, 세션화 검증, WAU 교차 검증은 직접 확인했습니다.


## 14. 한계 및 개선점

- CSV에서 Bronze로 적재하는 단계는 입력 파일 전체를 읽습니다. 입력 경로를 날짜별 또는 변경분 단위로 받을 수 있다면 이 단계도 더 효율적으로 증분화할 수 있습니다.

- Gold는 현재 풀리프레시 방식입니다. 데이터 기간이 길어지고 매일 갱신해야 한다면, 영향받은 주만 다시 계산하는 증분 방식으로 개선할 수 있습니다.

- 특정 `user_id`에 이벤트가 과도하게 몰릴 경우, 세션화 과정에서 파티션 스큐가 발생할 수 있습니다. 운영 환경에서는 Spark AQE skew 처리나 heavy user 분리 전략을 고려할 수 있습니다.

- 이번 구현은 로컬 재현성을 위해 Spark local metastore를 사용했습니다. 운영 환경에서는 여러 Spark Job과 쿼리 엔진이 동일한 메타데이터를 공유할 수 있도록 Remote Hive Metastore 또는 AWS Glue Data Catalog로 전환하는 것이 적절합니다.

- 이번 과제에서는 요구사항에 맞춰 Parquet + Hive External Table 방식으로 구현했습니다. 운영 환경에서 더 강한 데이터 변경 이력 관리나 롤백 요구가 생긴다면 별도 테이블 포맷 검토가 필요할 수 있습니다.