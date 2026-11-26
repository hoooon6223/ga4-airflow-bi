# 구현 계획

작성일: 2026-11-25
기준 문서: [`01_design.md`](01_design.md), [`02_metric_definitions.md`](02_metric_definitions.md)

## 진행 방식

- 한 단계씩 진행한다. 각 단계의 **검증**을 통과해야 다음 단계로 넘어간다.
- **🧑 오너 확인** 표시가 있는 곳에서는 멈추고, 오너가 결과를 본 뒤 진행한다. 도구를 왜 이렇게 쓰는지 이해하는 것이 프로젝트 목표의 일부이기 때문이다.
- 순서는 **dbt → 대시보드 → Airflow**다. 변환이 맞는지 먼저 확인하고, 마지막에 자동화로 감싼다. Airflow를 먼저 띄우면 SQL 오류와 오케스트레이션 오류가 섞여서 원인을 찾기 어렵다.

## 현재 환경 (2026-11-25 확인)

| 항목 | 상태 | 필요한 조치 |
|---|---|---|
| Docker | **설치 안 됨** | 0단계에서 설치 (오너 작업) |
| Python | 3.13.3 | dbt는 별도 가상환경에 설치 |
| dbt | 설치 안 됨 | 1단계에서 설치 |
| gcloud / bq | 설치됨. 기본 프로젝트 `bigquery-457902` | — |
| GCP 인증 (ADC) | **없음** | 0단계에서 서비스 계정 생성 (오너 작업) |
| 메모리 | 48GB | Airflow 권장 사양(4GB 이상) 충분 |
| GitHub | `gh` 설치됨, 기존 저장소 `hoooon6223/ga4-ops-bi` | 새 저장소 `ga4-airflow-bi` 생성 |

## 폴더 구조 (목표)

```text
ga4-airflow-bi/
├── docs/                  설계, 정의서, 계획, 사고 기록
├── dbt/                   dbt 프로젝트
│   ├── models/staging/    stg_events
│   ├── models/mart/       f_orders, f_sessions, kpi_daily, kpi_weekly, dq_daily
│   ├── seeds/             data_incidents.csv
│   ├── tests/             품질 게이트 (warn / error)
│   └── analyses/          기준값 계산용 일회성 SQL
├── dashboard/             HTML 생성 스크립트와 템플릿
├── airflow/               docker-compose.yaml, Dockerfile, dags/
└── site/                  생성된 HTML (GitHub Pages로 게시)
```

비밀 정보(서비스 계정 키, GitHub 토큰)는 저장소 밖에 두거나 `.gitignore`로 제외한다. 커밋하지 않는다.

---

## 0단계. 환경 준비

| # | 작업 | 담당 | 검증 |
|---|---|---|---|
| 0-1 | Docker Desktop 설치 | 🧑 오너 | `docker compose version`이 출력된다 |
| 0-2 | GCP 서비스 계정 생성. 권한은 BigQuery Data Editor와 Job User. 키 JSON을 저장소 밖(예: `~/.gcp/ga4-airflow-bi.json`)에 저장 | 🧑 오너 | 그 키로 `bq ls bigquery-457902:` 조회 성공 |
| 0-3 | 프로젝트 폴더 git 초기화, `.gitignore` (키, `.env`, `target/`, `logs/`) | Claude | `git status`에 비밀 파일이 보이지 않는다 |
| 0-4 | BigQuery 데이터셋 생성: `ga4_staging`, `ga4_mart`, `ga4_mart_test` (위치 US, 원천과 같음) | Claude | `bq ls`에 세 데이터셋이 보인다. 기존 `ga4_ops_bi`는 그대로다 |

## 1단계. dbt 시작과 staging

| # | 작업 | 검증 |
|---|---|---|
| 1-1 | 가상환경에 `dbt-core` + `dbt-bigquery` 설치, `dbt init`, profiles 설정 (서비스 계정 키, 운영 target `ga4_mart` / 테스트 target `ga4_mart_test`) | `dbt debug` 통과 |
| 1-2 | source 정의: `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` | `dbt ls`에 source가 보인다 |
| 1-3 | `stg_events` 모델: 배열 펼치기, 세션 키, device(데스크톱/모바일), session_number, 거래번호 없는 구매·빈 구매 플래그. 날짜 파티션, `insert_overwrite`, `var('run_date')`로 하루 처리. 최초 1회는 11/1~1/31 전체를 full-refresh | 날짜별 행 수가 raw `events_YYYYMMDD`의 행 수와 일치한다 (92일 전부) |
| 1-4 | 같은 날짜를 두 번 실행 | 두 번째 실행 후에도 행 수와 합계가 같다 (멱등성) |

🧑 **오너 확인**: `ref`, `source`, 증분 모델, `insert_overwrite`가 실제로 무엇을 하는지 실행 결과로 본다.

## 2단계. 사실 테이블 (f_orders, f_sessions)

| # | 작업 | 검증 |
|---|---|---|
| 2-1 | `f_orders`: 거래번호당 첫 이벤트, 첫 세션 귀속, 이전 날짜 파티션에 이미 있는 거래번호 제외 (anti-join) | 운영 기간 56일을 날짜 순서대로 실행 → **주문 3,232건, 매출 $221,762** |
| 2-2 | `f_sessions`: 세션 × 날짜, 도달 플래그(조회·장바구니·결제·장바구니 경유 결제), 정상 주문 여부, 거래번호 없는 구매 여부, 주문 수, 매출 | **세션 230,471 / 조회 46,194 / 결제 6,192 (장바구니 경유 4,710) / 구매 세션 2,903** |
| 2-3 | dbt 기본 테스트: `f_orders.transaction_id` unique·not_null, `f_sessions` 키 unique | `dbt test` 통과 |
| 2-4 | 하루만 다시 실행 (예: 12/15) | 운영 기간 합계가 바뀌지 않는다 |

검증에 쓰는 기준 숫자는 설계 문서 §3.2, §3.3, §7에 있다. 차이가 나면 원인을 찾고, 의도된 차이(예: 날짜를 넘는 중복 처리)라면 문서에 기록한다.

## 3단계. KPI 테이블

| # | 작업 | 검증 |
|---|---|---|
| 3-1 | seed `data_incidents.csv` (11/1~11/29, 1/25~1/31 + 사유) | `dbt seed` 후 테이블 확인 |
| 3-2 | `kpi_daily`: 날짜 × 구분(전체 / 데스크톱 / 모바일 / 신규 / 재방문). K01~K05, F01~F13, S01 | 운영 기간 합계 기준: 신규 주문 1,125 / 재방문 2,107 / 데스크톱 1,868 / 모바일 1,364 |
| 3-3 | 비교 컬럼: 지난주 같은 요일 대비, 7일 평균 대비. 제외일 규칙 (비교 불가, 유효일 5일 미만이면 비움) | 12/7이 첫 비교일이다. 제외일을 하나 가짜로 넣었을 때 규칙대로 비워진다 |
| 3-4 | `kpi_weekly`: 월~일 8주, 세션은 주 단위 서로 다른 세션 수 | 주간 세션 ≤ 일별 세션의 합 |
| 3-5 | 항등식 테스트: K01 = K03 × K04 × K05, 구분별 합 = 전체 | `dbt test` 통과 |

🧑 **오너 확인**: `kpi_daily`를 직접 쿼리해 보고, 정의서와 같은 뜻으로 계산됐는지 본다.

## 4단계. 데이터 품질과 게이트

| # | 작업 | 검증 |
|---|---|---|
| 4-1 | `analyses/`에 기준값 계산 SQL 작성: DQ1(이벤트 총량), DQ3(이벤트별 세션), DQ4(매출 대사), DQ5(다중 주문 세션). 대상 기간 11/12~1/31 | 일별 분포표가 나온다 |
| 4-2 | 🧑 **오너 확인**: 분포를 보고 DQ1, DQ3, DQ4, DQ5 기준값을 함께 정한다. DQ2와 같은 방식(평소 최대치와 사고 사이, 최소 건수 조건)으로 정하고 정의서에 근거와 함께 기록 | 정의서의 [미정]이 모두 채워진다 |
| 4-3 | `dq_daily` 모델: 날짜 × 체크별 값, 기준, 판정, 진단 정보(기기·브라우저 분포, 사용자·세션 집중도) | 운영 기간 56일 모두 "차단" 0건. DQ2 하루 최대 14.3%(1/17) |
| 4-4 | 게이트 테스트: `tests/`에 severity `error`(차단), `warn`(경고) 테스트 | 운영 기간에 `dbt build`가 error 없이 끝난다 |
| 4-5 | **1/26 사고 재현**: 테스트 target(`ga4_mart_test`)에 1/26을 실행 | DQ2 = 60.8%로 error 테스트가 실패하고, `dbt build`의 종료 코드가 0이 아니다 |
| 4-6 | 모델과 컬럼 `description`에 정의서 문장을 옮기고 `dbt docs generate` | 문서 사이트에서 지표 정의가 보인다 |

## 5단계. 대시보드 HTML

| # | 작업 | 검증 |
|---|---|---|
| 5-1 | 🧑 **오너 확인**: 화면 초안(목업)을 보고 배치를 정한다. 위에서부터 ① 오늘 볼 것 ② 어디서 변했나 ③ 세부·품질 | 배치 확정 |
| 5-2 | `dashboard/render.py`: `kpi_daily`, `kpi_weekly`, `dq_daily`를 읽어 날짜별 HTML 생성 (Jinja 템플릿, Power BI 스타일) | 12/7, 12/30(정상 판정 사례)을 렌더링해서 숫자를 마트와 대조 |
| 5-3 | 차단 화면: 판정이 차단이면 KPI 대신 "검수 실패" 페이지 | 1/26 테스트 데이터로 "검수 실패" 페이지가 생성된다 |
| 5-4 | 각주: "빈 구매로 주문·매출 약 4~5% 과소 집계", "퍼널 구매 세션 < 주문 수", "비교 불가" 표시 | 화면에서 확인 |
| 5-5 | 목록 페이지(날짜별 아카이브) | 링크가 모두 열린다 |

## 6단계. Airflow

| # | 작업 | 검증 |
|---|---|---|
| 6-1 | 공식 `docker-compose.yaml` 기반 구성. 커스텀 Dockerfile에서 dbt를 **별도 가상환경**에 설치 (Airflow와 dbt의 패키지 충돌 방지). 서비스 계정 키는 볼륨으로 마운트 | `docker compose up` 후 Airflow UI 접속 |
| 6-2 | DAG `ga4_ops_daily`: `check_raw`(`BigQueryTableExistenceSensor`, 10분 간격·6시간 대기 후 실패) → `dbt_run` → `dbt_test`(게이트) → `render_dashboard` → `publish`. `dbt build`가 아니라 run/test로 나눈다 (설계 문서 §8). 실패 시 알림 콜백 (기본은 로그, Slack webhook은 선택) | 그래프 화면에 작업이 순서대로 보인다 |
| 6-3 | 날짜 대응: `data_date = logical_date − offset`. offset은 Airflow Variable로 둔다 (켜는 날 정함). `catchup=True`, `max_active_runs=1`, `depends_on_past=False`, 스케줄 07:00 KST | 수동 실행 시 의도한 데이터 날짜가 로그에 찍힌다 |
| 6-4 | 개발용으로 짧게 확인: offset을 임시로 잡고 3일치만 catchup | 3일이 날짜 순서대로 하나씩 성공한다 |
| 6-5 | 차단 시나리오: 테스트 target으로 1/26을 수동 실행 | `dbt_build` 실패 → `render_dashboard`·`publish`는 실행되지 않음(upstream_failed) → 검수 실패 페이지만 생성, 알림 로그 확인 |
| 6-6 | 멱등성: 같은 날짜를 Airflow에서 "Clear"로 다시 실행 | 마트 결과가 같다 |

🧑 **오너 확인**: Airflow UI에서 DAG 그래프, 날짜별 실행 이력, 실패한 작업의 로그를 직접 본다.

## 7단계. 게시 (GitHub Pages)

| # | 작업 | 담당 | 검증 |
|---|---|---|---|
| 7-1 | 새 GitHub 저장소 `ga4-airflow-bi` 생성, Pages 설정 | Claude (생성 전 확인 받음) | 저장소와 Pages 주소가 생긴다 |
| 7-2 | 컨테이너에서 push할 GitHub 토큰 (저장소 한정, 쓰기 권한) | 🧑 오너 | 토큰이 `.env`에만 있고 커밋되지 않는다 |
| 7-3 | `publish` 작업: `site/`를 커밋하고 push | Claude | 테스트 날짜 하나가 Pages 주소에 뜬다 |

## 8단계. 실제 가동 (백필 42일 + 라이브 14일)

| # | 작업 | 검증 |
|---|---|---|
| 8-1 | 가동일 결정 → offset 계산 (가동일 = 데이터 1/11에 대응). 개발용 마트 데이터를 비우고 시작 | offset 값과 첫 라이브 날짜를 기록 |
| 8-2 | DAG 활성화 → 11/30~1/10의 42일 catchup (백필) | 42개 실행이 모두 성공. 운영 기간 기준 숫자 중 1/10까지의 부분합이 2단계 결과와 같다 |
| 8-3 | 라이브 14일: 매일 07:00 KST 실행 (데이터 1/11~1/24) | 날짜별 실행 이력. 노트북이 꺼져 있던 날은 다시 켰을 때 catchup으로 처리된다 |
| 8-4 | 마지막 날 이후 | 운영 기간 56일 전체가 2단계·3단계 기준 숫자와 일치한다 |

## 9단계. 포트폴리오 정리

| # | 작업 | 검증 |
|---|---|---|
| 9-1 | README: 목적 → 데이터 상태 → 정의 → 품질 게이트 → 도구 선택(ADR) → 결과 화면 | 🧑 오너 검토 |
| 9-2 | `docs/04_incident_0126.md`: 1/26 사고 재현 기록 (감지 → 위치 판별 → 요청 내용 → 처리) | 🧑 오너 검토 |
| 9-3 | `ga4-ops-bi`(GitHub Actions)와의 비교: 시작 지연, 백필, 게이트 구현 방식 | 🧑 오너 검토 |

---

## 위험과 대응

| 위험 | 영향 | 대응 |
|---|---|---|
| Airflow 3의 기본값 변화 (catchup 기본값 false 등) | 백필이 안 됨 | DAG에 `catchup=True`를 명시하고, 6-4에서 확인 |
| dbt와 Airflow의 패키지 충돌 | 이미지 빌드 실패 | dbt를 별도 가상환경에 설치 (6-1) |
| BigQuery 비용 | 쿼리 요금 | 원천 테이블 하나가 하루 수십 MB 수준이다. `stg_events` 이후는 파티션을 읽어서 비용이 작다. full-refresh는 1회만 |
| 노트북 잠자기 | 라이브 실행 지연 | catchup으로 따라잡는다. 지연 자체를 실행 이력으로 기록 |
| 비밀 정보 유출 | 계정 위험 | 키와 토큰은 저장소 밖이나 `.env`에만 두고 `.gitignore`로 제외. 커밋 전 확인 |
