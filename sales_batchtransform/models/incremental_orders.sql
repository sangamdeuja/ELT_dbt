{{
    config(
        materialized='incremental',
        unique_key='OrderId',
        alias='orders'
    )
}}

with
    last_run
    as
    (
        select coalesce(max(ChangeTime), '1900-01-01') as last_change_time
        from {{ this }}
    )
,
    incremental_data
    as
    (
        select *
        from {{ ref
    ('stg_orders') }}
    
    {%
if is_incremental() %}
    where ChangeTime >
(select last_change_time
from last_run)
{% endif %}
)
, final as
(
    select distinct
    OrderId,
    CustomerId,
    OrderDate,
    OrderStatus,
    ProductId,
    QuantityOrdered,
    Price,
    TotalAmount,
    ChangeTime,
    '{{ invocation_id }}' as batch_id
from incremental_data
)
select * from final
