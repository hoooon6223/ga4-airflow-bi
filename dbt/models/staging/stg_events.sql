{#
  1행 = 이벤트 1건. 배열(event_params, items)을 한 번만 펼쳐서, 아래 모델이 UNNEST를 반복하지 않게 한다.
  결함 구간을 포함해 전 기간을 보관한다 (최초 1회: start_date~end_date 전체, 이후: run_date 하루).
  insert_overwrite: 이번 실행에 들어온 날짜의 파티션만 통째로 교체 → 같은 날을 다시 돌려도 결과가 같다.
#}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'event_date', 'data_type': 'date'},
    cluster_by = ['event_name']
) }}

{%- set start_date = var('start_date', var('run_date')) -%}
{%- set end_date = var('end_date', var('run_date')) -%}

with src as (
    select *
    from {{ source('ga4', 'events') }}
    where _table_suffix between format_date('%Y%m%d', date '{{ start_date }}')
                            and format_date('%Y%m%d', date '{{ end_date }}')
)

select
    parse_date('%Y%m%d', event_date)                                         as event_date,
    timestamp_micros(event_timestamp)                                        as event_ts,
    event_name,
    user_pseudo_id,
    (select value.int_value from unnest(event_params) where key = 'ga_session_id')     as ga_session_id,
    concat(user_pseudo_id, '-', cast(
        (select value.int_value from unnest(event_params) where key = 'ga_session_id') as string
    ))                                                                       as session_key,
    (select value.int_value from unnest(event_params) where key = 'ga_session_number') as session_number,

    device.category                                                          as device_category,
    if(device.category = 'desktop', 'desktop', 'mobile')                     as device_group,  -- tablet → mobile
    device.web_info.browser                                                  as browser,

    -- purchase fields (null on non-purchase events)
    ecommerce.transaction_id                                                 as transaction_id,
    ecommerce.purchase_revenue                                               as purchase_revenue,
    ecommerce.tax_value                                                      as tax_value,
    (select sum(item_revenue) from unnest(items))                            as item_revenue,

    event_name = 'purchase'                                                  as is_purchase,
    event_name = 'purchase'
        and ifnull(ecommerce.transaction_id, '') in ('', '(not set)')        as is_no_tid_purchase,  -- DQ2 numerator
    event_name = 'purchase'
        and ifnull(ecommerce.transaction_id, '') in ('', '(not set)')
        and ecommerce.purchase_revenue is null                               as is_empty_purchase   -- 빈 구매
from src
