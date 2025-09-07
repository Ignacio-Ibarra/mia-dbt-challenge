{{ config(materialized='view') }}


with data_cast as (

SELECT 
    STRPTIME(file_date, '%Y%m%d') AS fecha_file,
    *
    FROM {{ref('normalize_brasil')}}
)

SELECT
    {{ select_except(data_cast, ['file_date']) }}
FROM data_cast;
