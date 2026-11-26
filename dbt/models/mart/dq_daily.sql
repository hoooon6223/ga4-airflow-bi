{#
  1행 = 날짜 × 품질 체크. 판정(status): ok / warn / block / insufficient_baseline.
  기준값은 docs/02_metric_definitions.md (근거: analyses/dq_calibration_daily.sql, 2026-11-25).
  - stg_events만 읽는다 → mart 테이블과 독립적으로 판정한다 (테스트 target에서도 기준선이 있다).
  - 기준선 = 직전 7일 중 결함 구간·이전 차단일이 아닌 날의 평균, 유효일 5일 이상일 때만.
  - 판정에는 그날까지의 데이터만 쓴다.
  - DQ6(집계 대사)는 이 테이블이 아니라 dbt 테스트(tests/dq6_*.sql)로 판정한다.
  게이트: tests/gate_block.sql(error)이 status = 'block'을 찾으면 dbt test가 실패 → Airflow가 게시를 멈춘다.
#}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'dq_date', 'data_type': 'date'}
) }}

{%- set start_date = var('start_date', var('run_date')) -%}
{%- set end_date = var('end_date', var('run_date')) -%}

{#- thresholds (초기값) -#}
{%- set dq1_block_ratio = 0.3 -%}
{%- set dq2_warn = 0.2 -%}{%- set dq2_block = 0.4 -%}{%- set dq2_min_sessions = 5 -%}
{%- set dq3_warn_ratio = 0.1 -%}{%- set dq3_min_baseline = 20 -%}
{%- set dq4_warn_gap = 0.02 -%}
{%- set dq5_warn_share = 0.25 -%}{%- set dq5_min_sessions = 5 -%}
{%- set min_baseline_days = 5 -%}

with days as (   -- 데이터가 0건인 날도 행이 있어야 "미도착"을 판정할 수 있다
    select d as dq_date
    from unnest(generate_date_array(date_sub(date '{{ start_date }}', interval 7 day), date '{{ end_date }}')) d
),

events as (
    select *
    from {{ ref('stg_events') }}
    where event_date between date_sub(date '{{ start_date }}', interval 7 day) and date '{{ end_date }}'
),

sessions as (
    select
        event_date, session_key,
        any_value(device_group)                                         as device_group,
        any_value(browser)                                              as browser,
        any_value(user_pseudo_id)                                       as user_pseudo_id,
        logical_or(event_name = 'view_item')                            as vi,
        logical_or(event_name = 'add_to_cart')                          as atc,
        logical_or(event_name = 'begin_checkout')                       as bc,
        logical_or(is_purchase)                                         as pu,
        logical_or(is_no_tid_purchase)                                  as no_tid,
        countif(is_no_tid_purchase)                                     as no_tid_events,
        count(distinct if(is_purchase and not is_no_tid_purchase, transaction_id, null)) as tids
    from events
    group by 1, 2
),

orders as (
    select event_date, purchase_revenue, item_revenue
    from events
    where is_purchase and not is_no_tid_purchase
    qualify row_number() over (partition by event_date, transaction_id order by event_ts) = 1
),

daily as (
    select
        d.dq_date,
        coalesce(e.events_total, 0)        as events_total,
        coalesce(s.vi, 0)  as vi,  coalesce(s.atc, 0) as atc,
        coalesce(s.bc, 0)  as bc,  coalesce(s.pu, 0)  as pu,
        coalesce(s.no_tid, 0)              as no_tid,
        coalesce(s.order_sessions, 0)      as order_sessions,
        coalesce(s.multi_order, 0)         as multi_order,
        r.purchase_revenue, r.item_revenue
    from days d
    left join (select event_date, count(*) as events_total from events group by 1) e on e.event_date = d.dq_date
    left join (
        select event_date,
               countif(vi) as vi, countif(atc) as atc, countif(bc) as bc, countif(pu) as pu,
               countif(pu and no_tid) as no_tid, countif(tids > 0) as order_sessions, countif(tids > 1) as multi_order
        from sessions group by 1
    ) s on s.event_date = d.dq_date
    left join (
        select event_date, sum(purchase_revenue) as purchase_revenue, sum(item_revenue) as item_revenue
        from orders group by 1
    ) r on r.event_date = d.dq_date
),

excluded_days as (
    select d as dq_date from {{ ref('data_incidents') }}, unnest(generate_date_array(start_date, end_date)) d
    {% if is_incremental() %}
    union distinct
    select dq_date from {{ this }}
    where status = 'block' and dq_date < date '{{ start_date }}'
    {% endif %}
),

valid_daily as (
    select d.*
    from daily d
    left join excluded_days x using (dq_date)
    where x.dq_date is null
),

baseline as (
    select t.dq_date,
           count(b.dq_date)    as baseline_days,
           avg(b.events_total) as events_total, avg(b.vi) as vi, avg(b.atc) as atc,
           avg(b.bc) as bc, avg(b.pu) as pu
    from daily t
    left join valid_daily b
      on b.dq_date between date_sub(t.dq_date, interval 7 day) and date_sub(t.dq_date, interval 1 day)
    group by 1
),

diag as (   -- DQ2 진단 정보: 거래번호 없는 구매가 어디에 몰렸나 (판정에는 쓰지 않음)
    select
        event_date as dq_date,
        format('users=%d sessions=%d events=%d | desktop=%d mobile=%d | browsers: %s',
               count(distinct user_pseudo_id), count(*), sum(no_tid_events),
               countif(device_group = 'desktop'), countif(device_group = 'mobile'),
               string_agg(distinct browser, ',' order by browser)) as detail
    from sessions
    where no_tid
    group by 1
),

target as (
    select t.*, b.baseline_days, b.events_total as b_events, b.vi as b_vi, b.atc as b_atc, b.bc as b_bc, b.pu as b_pu
    from daily t join baseline b using (dq_date)
    where t.dq_date between date '{{ start_date }}' and date '{{ end_date }}'
),

checks as (
    -- DQ1 데이터 도착 / 이벤트 총량
    select dq_date, 'DQ1' as check_id, 'events_total_vs_7d' as check_name,
           safe_divide(events_total, b_events) as value, events_total as numerator, b_events as denominator,
           case when events_total = 0 then 'block'
                when baseline_days < {{ min_baseline_days }} then 'insufficient_baseline'
                when safe_divide(events_total, b_events) < {{ dq1_block_ratio }} then 'block'
                else 'ok' end as status,
           cast(null as string) as detail
    from target

    union all
    -- DQ2 거래번호 없는 구매 세션 비율
    select t.dq_date, 'DQ2', 'no_tid_purchase_session_share',
           safe_divide(no_tid, pu), no_tid, pu,
           case when no_tid >= {{ dq2_min_sessions }} and safe_divide(no_tid, pu) >= {{ dq2_block }} then 'block'
                when no_tid >= {{ dq2_min_sessions }} and safe_divide(no_tid, pu) >= {{ dq2_warn }} then 'warn'
                else 'ok' end,
           g.detail
    from target t left join diag g using (dq_date)

    {% for ev in ['vi', 'atc', 'bc', 'pu'] %}
    union all
    -- DQ3 이벤트별 수집량 급감
    select dq_date, 'DQ3', '{{ {"vi": "view_item", "atc": "add_to_cart", "bc": "begin_checkout", "pu": "purchase"}[ev] }}_sessions_vs_7d',
           safe_divide({{ ev }}, b_{{ ev }}), {{ ev }}, b_{{ ev }},
           case when baseline_days < {{ min_baseline_days }} then 'insufficient_baseline'
                when b_{{ ev }} >= {{ dq3_min_baseline }} and safe_divide({{ ev }}, b_{{ ev }}) < {{ dq3_warn_ratio }} then 'warn'
                else 'ok' end,
           cast(null as string)
    from target
    {% endfor %}

    union all
    -- DQ4 주문 매출 대사
    select dq_date, 'DQ4', 'purchase_vs_item_revenue_gap',
           safe_divide(abs(purchase_revenue - item_revenue), purchase_revenue), purchase_revenue, item_revenue,
           if(safe_divide(abs(purchase_revenue - item_revenue), purchase_revenue) > {{ dq4_warn_gap }}, 'warn', 'ok'),
           cast(null as string)
    from target

    union all
    -- DQ5 다중 주문 세션 비율
    select dq_date, 'DQ5', 'multi_order_session_share',
           safe_divide(multi_order, order_sessions), multi_order, order_sessions,
           if(multi_order >= {{ dq5_min_sessions }} and safe_divide(multi_order, order_sessions) >= {{ dq5_warn_share }}, 'warn', 'ok'),
           cast(null as string)
    from target
)

select
    c.dq_date,
    c.check_id,
    c.check_name,
    c.value,
    c.numerator,
    c.denominator,
    c.status,
    c.detail,
    t.baseline_days
from checks c
join target t using (dq_date)
