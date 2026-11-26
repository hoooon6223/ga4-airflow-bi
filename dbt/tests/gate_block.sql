{#
  품질 게이트 — 차단 (error). 이번 실행 날짜에 status = 'block'인 체크가 하나라도 있으면 실패.
  dbt test가 실패하면 Airflow의 다음 작업(대시보드 생성·게시)이 실행되지 않는다.
#}
{{ config(severity = 'error') }}

select dq_date, check_id, check_name, value, numerator, denominator, detail
from {{ ref('dq_daily') }}
where status = 'block'
  and dq_date between date '{{ var("start_date", var("run_date")) }}' and date '{{ var("end_date", var("run_date")) }}'
