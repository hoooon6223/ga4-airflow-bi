{#
  1행 = 세션 × 날짜. 자정을 넘긴 세션은 이벤트가 있었던 날짜마다 1행씩 (전체 세션의 0.23%).
  퍼널 단계는 "그 세션·날짜에 해당 이벤트가 1개 이상 있으면 도달"로 센다 (앞 단계를 건너뛰어도 셈).
  주문은 f_orders에서 이 세션·날짜에 귀속된 것만 붙인다.
#}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'session_date', 'data_type': 'date'}
) }}

{%- set start_date = var('start_date', var('run_date')) -%}
{%- set end_date = var('end_date', var('run_date')) -%}

with events as (
    select *
    from {{ ref('stg_events') }}
    where event_date between date '{{ start_date }}' and date '{{ end_date }}'
),

sessions as (
    select
        event_date                                           as session_date,
        session_key,
        any_value(user_pseudo_id)                            as user_pseudo_id,
        any_value(device_group)                              as device_group,
        any_value(browser)                                   as browser,
        min(session_number) = 1                              as is_new_session,
        logical_or(event_name = 'view_item')                 as has_view_item,
        logical_or(event_name = 'add_to_cart')               as has_add_to_cart,
        logical_or(event_name = 'begin_checkout')            as has_begin_checkout,
        logical_or(is_purchase)                              as has_purchase_event,
        logical_or(is_no_tid_purchase)                       as has_no_tid_purchase,
        logical_or(is_empty_purchase)                        as has_empty_purchase
    from events
    group by session_date, session_key
),

orders as (
    select order_date, session_key, count(*) as orders, sum(purchase_revenue) as revenue
    from {{ ref('f_orders') }}
    where order_date between date '{{ start_date }}' and date '{{ end_date }}'
    group by order_date, session_key
)

select
    s.session_date,
    s.session_key,
    concat(s.session_key, '|', cast(s.session_date as string))      as session_day_key,
    s.user_pseudo_id,
    s.device_group,
    s.browser,
    s.is_new_session,
    s.has_view_item,
    s.has_add_to_cart,
    s.has_begin_checkout,
    s.has_begin_checkout and s.has_add_to_cart                       as is_checkout_with_cart,
    s.has_begin_checkout and not s.has_add_to_cart                   as is_checkout_direct,
    s.has_purchase_event,
    s.has_no_tid_purchase,
    s.has_empty_purchase,
    coalesce(o.orders, 0)                                            as orders,
    coalesce(o.revenue, 0)                                           as revenue,
    coalesce(o.orders, 0) > 0                                        as has_valid_order
from sessions s
left join orders o
  on o.order_date = s.session_date
 and o.session_key = s.session_key
