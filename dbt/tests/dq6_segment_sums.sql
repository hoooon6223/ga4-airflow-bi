{#
  DQ6 집계 대사 (차단): 기기별 합 = 전체, 신규+재방문 = 전체. 행이 나오면 실패.
  이번 실행 날짜만 검사한다 (지난 날의 문제로 오늘이 막히지 않게).
#}
{{ config(severity = 'error') }}

{%- set start_date = var('start_date', var('run_date')) -%}
{%- set end_date = var('end_date', var('run_date')) -%}

with k as (
    select * from {{ ref('kpi_daily') }}
    where kpi_date between date '{{ start_date }}' and date '{{ end_date }}'
),
total as (
    select kpi_date, sessions, orders, revenue from k where segment_type = 'all'
),
parts as (
    select kpi_date, segment_type,
           sum(sessions) as sessions, sum(orders) as orders, sum(revenue) as revenue
    from k where segment_type != 'all'
    group by 1, 2
)
select p.kpi_date, p.segment_type,
       p.sessions as part_sessions, t.sessions as total_sessions,
       p.orders as part_orders, t.orders as total_orders,
       p.revenue as part_revenue, t.revenue as total_revenue
from parts p
join total t using (kpi_date)
where p.sessions != t.sessions
   or p.orders != t.orders
   or abs(p.revenue - t.revenue) > 0.01
