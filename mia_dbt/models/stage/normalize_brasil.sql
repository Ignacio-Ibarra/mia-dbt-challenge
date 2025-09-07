{{ config(materialized='view') }}

with raw as (
    select
        {{ normalize_all_columns('brasil') }}
    from {{ source('raw_data', 'brasil') }}
)

select * from raw

