{{ config(materialized='view') }}

with raw as (
    select
        {{ normalize_all_columns('mexico') }}
    from {{ source('raw_data', 'mexico') }}
)

select * from raw