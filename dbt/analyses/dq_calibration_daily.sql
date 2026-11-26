{#
  품질 체크 기준값 계산용 (일회성). 11/1~1/31 전체를 날짜별로 보고,
  평소(운영 기간) 분포와 알려진 사고 사이에 기준을 둔다.
  baseline = 직전 7일 중 결함 구간이 아닌 날의 평균 (파이프라인 비교 규칙과 같음, 유효일 5일 이상).
  실행: dbt compile -s dq_calibration_daily → target/compiled/.../dq_calibration_daily.sql 을 BigQuery에서 실행
#}
with events as (
    select * from {{ ref('stg_events') }}
),

sessions as (
    select
        event_date, session_key,
        logical_or(event_name = 'view_item')      as vi,
        logical_or(event_name = 'add_to_cart')    as atc,
        logical_or(event_name = 'begin_checkout') as bc,
        logical_or(is_purchase)                   as pu,
        logical_or(is_no_tid_purchase)            as no_tid,
        count(distinct if(is_purchase and not is_no_tid_purchase, transaction_id, null)) as tids
    from events
    group by 1, 2
),

orders as (   -- 거래번호당 그날 첫 이벤트
    select event_date, purchase_revenue, item_revenue
    from events
    where is_purchase and not is_no_tid_purchase
    qualify row_number() over (partition by event_date, transaction_id order by event_ts) = 1
),

daily as (
    select
        s.event_date,
        (select count(*) from events e where e.event_date = s.event_date)  as events_total,   -- DQ1
        countif(vi)  as vi_sessions,                                                         -- DQ3
        countif(atc) as atc_sessions,
        countif(bc)  as bc_sessions,
        countif(pu)  as pu_sessions,
        countif(pu and no_tid)                                             as no_tid_sessions, -- DQ2
        countif(tids > 0)                                                  as order_sessions,  -- DQ5 denominator
        countif(tids > 1)                                                  as multi_order_sessions
    from sessions s
    group by 1
),

rev as (
    select event_date, sum(purchase_revenue) as purchase_revenue, sum(item_revenue) as item_revenue
    from orders group by 1
),

incident_days as (
    select d from {{ ref('data_incidents') }}, unnest(generate_date_array(start_date, end_date)) d
),

with_flags as (
    select d.*, r.purchase_revenue, r.item_revenue, i.d is not null as is_incident
    from daily d
    left join rev r using (event_date)
    left join incident_days i on i.d = d.event_date
)

select
    t.event_date,
    t.is_incident,
    t.events_total,
    t.vi_sessions, t.atc_sessions, t.bc_sessions, t.pu_sessions,
    count(b.event_date)                                                    as baseline_days,
    safe_divide(t.events_total, avg(b.events_total))                       as dq1_events_ratio,
    safe_divide(t.vi_sessions,  avg(b.vi_sessions))                        as dq3_view_ratio,
    safe_divide(t.atc_sessions, avg(b.atc_sessions))                       as dq3_cart_ratio,
    safe_divide(t.bc_sessions,  avg(b.bc_sessions))                        as dq3_checkout_ratio,
    safe_divide(t.pu_sessions,  avg(b.pu_sessions))                        as dq3_purchase_ratio,
    safe_divide(t.no_tid_sessions, t.pu_sessions)                          as dq2_ratio,
    t.no_tid_sessions,
    safe_divide(abs(t.purchase_revenue - t.item_revenue), t.purchase_revenue) as dq4_revenue_gap,
    safe_divide(t.multi_order_sessions, t.order_sessions)                  as dq5_multi_order_share,
    t.multi_order_sessions,
    t.order_sessions
from with_flags t
left join with_flags b
  on b.event_date between date_sub(t.event_date, interval 7 day) and date_sub(t.event_date, interval 1 day)
 and not b.is_incident
group by t.event_date, t.is_incident, t.events_total, t.vi_sessions, t.atc_sessions, t.bc_sessions,
         t.pu_sessions, t.no_tid_sessions, t.purchase_revenue, t.item_revenue,
         t.multi_order_sessions, t.order_sessions
order by t.event_date
