{{ config(materialized='table') }}

with raw as (
    select
        {{ normalize_all_columns(source('raw_data', 'mexico')) }}
    from {{ source('raw_data', 'mexico') }}
)

select * from raw