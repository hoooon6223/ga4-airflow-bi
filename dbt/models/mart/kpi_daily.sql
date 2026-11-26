{#
  1행 = 날짜 × 구분. 구분: all / device(desktop, mobile) / visitor(new, returning).
  지표 정의는 docs/02_metric_definitions.md (K01~K05, F01~F13, S01~S02).
  비교 기준:
    - 지난주 같은 요일: 그날이 유효하지 않으면(결함 구간·차단일) null = "비교 불가". 대체하지 않는다.
    - 7일 평균: 직전 7일(달력) 중 유효한 날만. 유효한 날이 5일 미만이면 null.
      비율 지표는 일별 비율의 평균이 아니라 합계의 비율(가중 평균)로 계산한다.
  비교에 쓰려고 run_date 앞 7일을 함께 읽고, 결과는 run_date(또는 start~end)만 쓴다.
#}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'kpi_date', 'data_type': 'date'}
) }}

{%- set start_date = var('start_date', var('run_date')) -%}
{%- set end_date = var('end_date', var('run_date')) -%}

with sessions as (
    select s.*, seg.segment_type, seg.segment_value
    from {{ ref('f_sessions') }} s
    cross join unnest([
        struct('all' as segment_type, 'all' as segment_value),
        struct('device', s.device_group),
        struct('visitor', if(s.is_new_session, 'new', 'returning'))
    ]) as seg
    where s.session_date between date_sub(date '{{ start_date }}', interval 7 day) and date '{{ end_date }}'
),

base as (
    select
        session_date                                   as kpi_date,
        segment_type,
        segment_value,
        count(*)                                       as sessions,               -- K03
        sum(orders)                                    as orders,                 -- K02
        sum(revenue)                                   as revenue,                -- K01
        countif(has_view_item)                         as view_sessions,          -- F01
        countif(has_begin_checkout)                    as checkout_sessions,      -- F02
        countif(is_checkout_with_cart)                 as checkout_with_cart,     -- F02a
        countif(is_checkout_direct)                    as checkout_direct,        -- F02b
        countif(has_valid_order)                       as purchase_sessions,      -- F03
        countif(has_add_to_cart)                       as cart_sessions,          -- S01 numerator
        countif(is_new_session)                        as new_sessions            -- S02 numerator
    from sessions
    group by 1, 2, 3
),

-- 유효한 날 = 결함 구간도 아니고 품질 게이트 차단일도 아닌 날
invalid_days as (
    select d as invalid_date
    from {{ ref('data_incidents') }},
    unnest(generate_date_array(start_date, end_date)) as d
    union distinct
    select dq_date
    from {{ ref('dq_daily') }}
    where status = 'block'
),

valid as (
    select b.*
    from base b
    left join invalid_days i on i.invalid_date = b.kpi_date
    where i.invalid_date is null
),

target_days as (
    select *
    from base
    where kpi_date between date '{{ start_date }}' and date '{{ end_date }}'
),

wow as (   -- 지난주 같은 요일 (유효한 날일 때만 join됨)
    select t.kpi_date, t.segment_type, t.segment_value,
           v.sessions as sessions_wow, v.orders as orders_wow, v.revenue as revenue_wow
    from target_days t
    join valid v
      on v.kpi_date = date_sub(t.kpi_date, interval 7 day)
     and v.segment_type = t.segment_type and v.segment_value = t.segment_value
),

avg7 as (  -- 직전 7일 중 유효한 날
    select t.kpi_date, t.segment_type, t.segment_value,
           count(*)                        as avg7_valid_days,
           sum(v.sessions)                 as sessions_7d,
           sum(v.orders)                   as orders_7d,
           sum(v.revenue)                  as revenue_7d
    from target_days t
    join valid v
      on v.kpi_date between date_sub(t.kpi_date, interval 7 day) and date_sub(t.kpi_date, interval 1 day)
     and v.segment_type = t.segment_type and v.segment_value = t.segment_value
    group by 1, 2, 3
)

select
    t.kpi_date,
    t.segment_type,
    t.segment_value,
    format_date('%a', t.kpi_date)                                  as weekday,

    -- KPI
    t.sessions,
    t.orders,
    round(t.revenue, 2)                                            as revenue,
    safe_divide(t.orders, t.sessions)                              as order_cr,           -- K04
    safe_divide(t.revenue, t.orders)                               as aov,                -- K05

    -- funnel
    t.view_sessions,
    t.checkout_sessions,
    t.checkout_with_cart,
    t.checkout_direct,
    t.purchase_sessions,
    safe_divide(t.view_sessions, t.sessions)                       as view_rate,          -- F10
    safe_divide(t.checkout_sessions, t.view_sessions)              as view_to_checkout,   -- F11
    safe_divide(t.purchase_sessions, t.checkout_sessions)          as checkout_to_purchase, -- F12
    safe_divide(t.checkout_direct, t.checkout_sessions)            as direct_checkout_share, -- F13

    -- supporting
    t.cart_sessions,
    safe_divide(t.cart_sessions, t.view_sessions)                  as cart_rate,          -- S01
    safe_divide(t.new_sessions, t.sessions)                        as new_session_share,  -- S02

    -- vs same weekday last week (null = 비교 불가)
    w.revenue_wow,
    safe_divide(t.revenue - w.revenue_wow, w.revenue_wow)          as revenue_wow_pct,
    w.orders_wow,
    w.sessions_wow,
    safe_divide(t.sessions - w.sessions_wow, w.sessions_wow)       as sessions_wow_pct,
    safe_divide(w.orders_wow, w.sessions_wow)                      as order_cr_wow,
    safe_divide(w.revenue_wow, w.orders_wow)                       as aov_wow,

    -- vs trailing 7-day average (null when fewer than 5 valid days)
    coalesce(a.avg7_valid_days, 0)                                 as avg7_valid_days,
    if(a.avg7_valid_days >= 5, a.revenue_7d / a.avg7_valid_days, null)   as revenue_avg7,
    if(a.avg7_valid_days >= 5,
       safe_divide(t.revenue - a.revenue_7d / a.avg7_valid_days, a.revenue_7d / a.avg7_valid_days),
       null)                                                       as revenue_avg7_pct,
    if(a.avg7_valid_days >= 5, a.sessions_7d / a.avg7_valid_days, null) as sessions_avg7,
    if(a.avg7_valid_days >= 5, a.orders_7d / a.avg7_valid_days, null)   as orders_avg7,
    if(a.avg7_valid_days >= 5, safe_divide(a.orders_7d, a.sessions_7d), null) as order_cr_avg7,
    if(a.avg7_valid_days >= 5, safe_divide(a.revenue_7d, a.orders_7d), null)  as aov_avg7,

    i.invalid_date is not null                                     as is_excluded_day   -- 결함 구간 또는 차단일
from target_days t
left join wow  w using (kpi_date, segment_type, segment_value)
left join avg7 a using (kpi_date, segment_type, segment_value)
left join invalid_days i on i.invalid_date = t.kpi_date
