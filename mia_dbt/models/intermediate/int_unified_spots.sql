-- mia_dbt/models/intermediate/int_unified_spots.sql
{{ config(materialized='view') }}

/*
    Modelo de unificación de spots de TV y Radio de Brasil y México
    
    Propósito:
    - Unificar estructuras de datos de ambos países
    - Estandarizar nomenclatura de medios y dimensiones
    - Preparar datos para estimación de costos y análisis
    
    Fuentes:
    - cast_mexico: Datos normalizados de México
    - cast_brasil: Datos normalizados de Brasil
*/

WITH mexico_spots AS (
    SELECT 
        -- Identificadores únicos
        CONCAT('MX_', fecha, '_', EXTRACT(HOUR FROM hora), '_', 
               ROW_NUMBER() OVER (ORDER BY fecha, hora, canal)) AS spot_id,
        
        -- Dimensiones temporales  
        fecha,
        hora,
        EXTRACT(HOUR FROM hora) AS hora_numero,
        CASE 
            WHEN EXTRACT(HOUR FROM hora) BETWEEN 8 AND 17 THEN 'DAY'
            WHEN EXTRACT(HOUR FROM hora) BETWEEN 18 AND 1 OR EXTRACT(HOUR FROM hora) = 1 THEN 'PRIMETIME'
            WHEN EXTRACT(HOUR FROM hora) BETWEEN 2 AND 7 THEN 'GREYTIME'
            ELSE 'OTROS'
        END AS segmento_horario,
        
        -- Dimensiones geográficas y de medio
        'MEXICO' AS pais,
        CASE 
            WHEN medio LIKE '%Radio%' THEN 'RADIO'
            WHEN medio LIKE '%Televisión%' THEN 'TELEVISION'
            ELSE 'OTRO'
        END AS medio_unificado,
        medio AS medio_original,
        canal AS canal_original,
        estacion_canal,
        
        -- Dimensiones de marca y producto
        marca AS marca_original,
        producto,
        "version" AS version_original,
        
        -- Métricas de duración y costo
        duracion_programada AS duracion_segundos,
        NULL AS costo_usd,
        TRUE AS costo_estimado_flag,
        
        -- Dimensiones de negocio
        cobertura,
        spot_tipo,
        sector,
        sub_sector AS subsector,
        categoria,
        
        -- Columnas específicas de México
        grupo_estacion,
        grupo_comercial,
        rango_horario,
        seg_truncados,
        localidad,
        
        -- Columnas específicas de Brasil (NULL para México)
        NULL AS plaza,
        NULL AS red,
        NULL AS operador,
        NULL AS programa,
        NULL AS evento,
        NULL AS agencia,
        NULL AS falla,
        NULL AS esprimera,
        
        -- Metadatos
        fecha_file,
        'MEXICO' AS fuente_pais
        
    FROM {{ ref('cast_mexico') }}
),

brasil_spots AS (
    SELECT 
        -- Identificadores únicos
        CONCAT('BR_', fecha, '_', EXTRACT(HOUR FROM CAST(hora AS TIME)), '_',
               ROW_NUMBER() OVER (ORDER BY fecha, hora, emisora)) AS spot_id,
        
        -- Dimensiones temporales
        fecha,
        CAST(hora AS TIME) AS hora,
        EXTRACT(HOUR FROM CAST(hora AS TIME)) AS hora_numero,
        CASE 
            WHEN EXTRACT(HOUR FROM CAST(hora AS TIME)) BETWEEN 8 AND 17 THEN 'DAY'
            WHEN EXTRACT(HOUR FROM CAST(hora AS TIME)) BETWEEN 18 AND 23 
                 OR EXTRACT(HOUR FROM CAST(hora AS TIME)) BETWEEN 0 AND 1 THEN 'PRIMETIME'
            WHEN EXTRACT(HOUR FROM CAST(hora AS TIME)) BETWEEN 2 AND 7 THEN 'GREYTIME'
            ELSE 'OTROS'
        END AS segmento_horario,
        
        -- Dimensiones geográficas y de medio
        pais,
        CASE 
            WHEN medio = 'RD' THEN 'RADIO'
            WHEN medio IN ('TC', 'TV') THEN 'TELEVISION'
            ELSE 'OTRO'
        END AS medio_unificado,
        medio AS medio_original,
        emisora AS canal_original,
        emisora AS estacion_canal,
        
        -- Dimensiones de marca y producto
        marca AS marca_original,
        producto AS producto,
        "version" AS version_original,
        
        -- Métricas de duración y costo
        duracion AS duracion_segundos,
        valordolar AS costo_usd,
        CASE WHEN valordolar IS NULL OR valordolar = 0 THEN TRUE ELSE FALSE END AS costo_estimado_flag,
        
        -- Dimensiones de negocio
        plaza AS cobertura,
        evento AS spot_tipo,
        sector,
        subsector,
               
        -- Columnas específicas de México (NULL para Brasil)
        NULL AS categoria,
        NULL AS grupo_estacion,
        NULL AS grupo_comercial,
        NULL AS rango_horario,
        NULL AS seg_truncados,
        NULL AS localidad,
        
        -- Columnas específicas de Brasil
        plaza,
        red,
        operador,
        programa,
        evento,
        agencia,
        falla,
        esprimera,
        
        -- Metadatos
        fecha_file,
        'BRASIL' AS fuente_pais
        
    FROM {{ ref('cast_brasil') }}
)

-- Unión de ambos datasets
SELECT 
    spot_id,
    fecha,
    hora,
    hora_numero,
    segmento_horario,
    pais,
    medio_unificado,
    medio_original,
    canal_original,
    estacion_canal,
    marca_original,
    producto,
    version_original,
    duracion_segundos,
    costo_usd,
    costo_estimado_flag,
    cobertura,
    spot_tipo,
    sector,
    subsector,
    categoria,
    
    -- Columnas específicas por país
    grupo_estacion,
    grupo_comercial,
    rango_horario,
    seg_truncados,
    localidad,
    plaza,
    red,
    operador,
    programa,
    evento,
    agencia,
    falla,
    esprimera,
    
    -- Metadatos
    fecha_file,
    fuente_pais

FROM mexico_spots

UNION ALL

SELECT 
    spot_id,
    fecha,
    hora,
    hora_numero,
    segmento_horario,
    pais,
    medio_unificado,
    medio_original,
    canal_original,
    estacion_canal,
    marca_original,
    producto,
    version_original,
    duracion_segundos,
    costo_usd,
    costo_estimado_flag,
    cobertura,
    spot_tipo,
    sector,
    subsector,
    categoria,
    
    -- Columnas específicas por país
    grupo_estacion,
    grupo_comercial,
    rango_horario,
    seg_truncados,
    localidad,
    plaza,
    red,
    operador,
    programa,
    evento,
    agencia,
    falla,
    esprimera,
    
    -- Metadatos
    fecha_file,
    fuente_pais

FROM brasil_spots