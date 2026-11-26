{#
  품질 게이트 — 경고 (warn). status = 'warn'인 체크가 있으면 경고만 남기고 통과.
  대시보드는 게시되고, 해당 지표 옆에 경고가 표시된다.
#}
{{ config(severity = 'warn') }}

select dq_date, check_id, check_name, value, numerator, denominator, detail
from {{ ref('dq_daily') }}
where status = 'warn'
  and dq_date between date '{{ var("start_date", var("run_date")) }}' and date '{{ var("end_date", var("run_date")) }}'
