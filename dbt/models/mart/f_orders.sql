{#
  1행 = 주문 1건 = 거래번호 1개.
  - 같은 거래번호가 여러 번 찍혔으면 처음 찍힌 이벤트를 쓴다 (그 이벤트의 세션에 귀속).
  - 이전 날짜 파티션에 이미 있는 거래번호는 주문으로 세지 않는다 (날짜를 넘는 중복, 운영 기간 7건).
    → 날짜 순서대로 돌리면 결과가 결정적이다 (Airflow max_active_runs=1).
  - 빈 구매·거래번호 없는 구매는 주문이 아니다 (DQ2에서 따로 센다).
  현업에서는 이 모델의 원천만 주문 DB로 바꾸면 된다.
#}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'order_date', 'data_type': 'date'}
) }}

{%- set start_date = var('start_date', var('run_date')) -%}
{%- set end_date = var('end_date', var('run_date')) -%}

with purchases as (
    select *
    from {{ ref('stg_events') }}
    where event_date between date '{{ start_date }}' and date '{{ end_date }}'
      and is_purchase
      and not is_no_tid_purchase
),

first_event as (
    select *
    from purchases
    qualify row_number() over (partition by transaction_id order by event_ts, session_key) = 1
)

select
    event_date                  as order_date,
    transaction_id,
    session_key,
    user_pseudo_id,
    event_ts                    as order_ts,
    purchase_revenue,
    item_revenue,
    tax_value,
    device_group,
    browser,
    session_number = 1          as is_new_session
from first_event
{% if is_incremental() %}
where transaction_id not in (
    select transaction_id from {{ this }} where order_date < date '{{ start_date }}'
)
{% endif %}
