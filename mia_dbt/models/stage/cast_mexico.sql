{{ config(materialized='view') }}


with data_cast as (

SELECT 
    CAST(STRPTIME(hora_gmt, '%Y-%m-%d %H:%M:%S %z') AS DATE) AS fecha,
    MAKE_TIME(
    EXTRACT(HOUR FROM STRPTIME(hora_gmt, '%Y-%m-%d %H:%M:%S %z')),
    EXTRACT(MINUTE FROM STRPTIME(hora_gmt, '%Y-%m-%d %H:%M:%S %z')),
    EXTRACT(SECOND FROM STRPTIME(hora_gmt, '%Y-%m-%d %H:%M:%S %z'))
    ) AS hora,
    STRPTIME(file_date, '%Y%m%d') AS fecha_file,
    'mexico' as pais,
    *
    FROM {{ref('normalize_mexico')}}
)


SELECT
    fecha_file,
    fecha,
    hora,
    pais,
    {{ select_except(ref('normalize_mexico'), ['file_date', 'hora_gmt'], alias='data_cast') }}
FROM data_cast

