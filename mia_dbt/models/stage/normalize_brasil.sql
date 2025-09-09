{{ config(materialized='table') }}

with raw as (
    select
        {{ normalize_all_columns(source('raw_data', 'brasil')) }}
    from {{ source('raw_data', 'brasil') }}
)

select * from raw

