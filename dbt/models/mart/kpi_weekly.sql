{#
  1행 = 주(월~일) × 구분. 하루를 처리할 때 그날이 속한 주 전체를 다시 계산한다.
  세션은 일별 합계가 아니라 그 주의 서로 다른 세션 수 (자정을 넘긴 세션을 두 번 세지 않음).
  퍼널 단계도 "그 주에 한 번이라도 도달한 세션" 기준.
  days_covered < 7이면 아직 진행 중인 주 (또는 운영 기간 경계의 주).
#}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'week_start', 'data_type': 'date'}
) }}

{%- set start_date = var('start_date', var('run_date')) -%}
{%- set end_date = var('end_date', var('run_date')) -%}

with sessions as (
    select
        date_trunc(s.session_date, week(monday)) as week_start,
        s.*,
        seg.segment_type,
        seg.segment_value
    from {{ ref('f_sessions') }} s
    cross join unnest([
        struct('all' as segment_type, 'all' as segment_value),
        struct('device', s.device_group),
        struct('visitor', if(s.is_new_session, 'new', 'returning'))
    ]) as seg
    where s.session_date between date_trunc(date '{{ start_date }}', week(monday))
                             and date '{{ end_date }}'
),

per_session as (   -- 한 세션이 주 안에서 여러 날에 걸쳐도 1개로
    select
        week_start, segment_type, segment_value, session_key,
        logical_or(has_view_item)          as has_view_item,
        logical_or(has_begin_checkout)     as has_begin_checkout,
        logical_or(has_add_to_cart)        as has_add_to_cart,
        logical_or(has_valid_order)        as has_valid_order,
        sum(orders)                        as orders,
        sum(revenue)                       as revenue
    from sessions
    group by 1, 2, 3, 4
),

days as (
    select week_start, count(distinct session_date) as days_covered
    from sessions
    group by 1
)

select
    p.week_start,
    date_add(p.week_start, interval 6 day)                         as week_end,
    p.segment_type,
    p.segment_value,
    d.days_covered,
    count(*)                                                       as sessions,
    sum(p.orders)                                                  as orders,
    round(sum(p.revenue), 2)                                       as revenue,
    safe_divide(sum(p.orders), count(*))                           as order_cr,
    safe_divide(sum(p.revenue), sum(p.orders))                     as aov,
    countif(p.has_view_item)                                       as view_sessions,
    countif(p.has_begin_checkout)                                  as checkout_sessions,
    countif(p.has_valid_order)                                     as purchase_sessions,
    countif(p.has_add_to_cart)                                     as cart_sessions,
    safe_divide(countif(p.has_add_to_cart), countif(p.has_view_item)) as cart_rate
from per_session p
join days d using (week_start)
group by p.week_start, p.segment_type, p.segment_value, d.days_covered
