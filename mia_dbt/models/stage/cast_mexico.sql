{{ config(materialized='view') }}


with data_cast as (

SELECT 
    CAST(STRPTIME(hora_gmt, '%Y-%m-%d %H:%M:%S %z') AS DATE) AS fecha,
    CAST(STRPTIME(hora_gmt, '%Y-%m-%d %H:%M:%S %z') AS TIME) AS hora,
    STRPTIME(file_date, '%Y%m%d') AS fecha_file,
    "mexico" as pais,
    *
    FROM {{ref('normalize_mexico')}}
)


SELECT
    {{ select_except(data_cast, ['file_date', 'hora_gmt']) }}
FROM data_cast;


