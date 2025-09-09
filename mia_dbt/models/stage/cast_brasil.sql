{{ config(materialized='view') }}

with data_cast as (
    select
        STRPTIME(file_date, '%Y%m%d') as fecha_file,
        *
    from {{ ref('normalize_brasil') }}
)

SELECT
    fecha_file,  -- la columna calculada la agregás manualmente
    {{ select_except(ref('normalize_brasil'), ['file_date'], alias='data_cast') }}
FROM data_cast
