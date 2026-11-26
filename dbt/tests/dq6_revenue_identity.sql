{#
  DQ6 집계 대사 (차단):
    1) 매출 = 세션 × 주문 전환율 × 객단가 (K01 = K03 × K04 × K05)
    2) kpi_daily 전체 구분의 주문·매출 = f_orders의 그날 합계 (빈 파티션·누락 감지)
  행이 나오면 실패.
#}
{{ config(severity = 'error') }}

{%- set start_date = var('start_date', var('run_date')) -%}
{%- set end_date = var('end_date', var('run_date')) -%}

with k as (
    select * from {{ ref('kpi_daily') }}
    where segment_type = 'all'
      and kpi_date between date '{{ start_date }}' and date '{{ end_date }}'
),
o as (
    select order_date as kpi_date, count(*) as orders, sum(purchase_revenue) as revenue
    from {{ ref('f_orders') }}
    where order_date between date '{{ start_date }}' and date '{{ end_date }}'
    group by 1
)
select k.kpi_date, 'identity' as check_name, k.revenue as left_value,
       k.sessions * k.order_cr * k.aov as right_value
from k
where k.orders > 0 and abs(k.revenue - k.sessions * k.order_cr * k.aov) > 0.01

union all

select coalesce(k.kpi_date, o.kpi_date), 'kpi_vs_orders', k.revenue, o.revenue
from k
full outer join o using (kpi_date)
where coalesce(k.orders, -1) != coalesce(o.orders, 0)
   or abs(coalesce(k.revenue, 0) - coalesce(round(o.revenue, 2), 0)) > 0.01
