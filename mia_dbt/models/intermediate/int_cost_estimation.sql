-- mia_dbt/models/intermediate/int_cost_estimation.sql
{{ config(materialized='view') }}

/*
    Modelo para estimar costos faltantes por segmento horario
    
    Propósito:
    - Calcular costos promedio por segmento horario (Day, Primetime, Greytime)
    - Estimar costos faltantes basándose en el promedio del segmento
    - Mantener trazabilidad de costos estimados vs reales
    
    Fuente:
    - int_unified_spots: Datos unificados de Brasil y México
*/

WITH cost_averages_by_segment AS (
    SELECT 
        segmento_horario,
        medio_unificado,
        pais,
        canal_original,
        AVG(costo_usd) AS costo_promedio_segmento,
        COUNT(*) AS total_spots_con_costo
    FROM {{ ref('int_unified_spots') }}
    WHERE costo_usd IS NOT NULL 
      AND costo_usd > 0
    GROUP BY segmento_horario, medio_unificado, pais, canal_original
),

cost_averages_fallback AS (
    -- Fallback: promedio general por segmento si no hay datos por canal
    SELECT 
        segmento_horario,
        medio_unificado,
        pais,
        NULL AS canal_original,
        AVG(costo_usd) AS costo_promedio_segmento,
        COUNT(*) AS total_spots_con_costo
    FROM {{ ref('int_unified_spots') }}
    WHERE costo_usd IS NOT NULL 
      AND costo_usd > 0
    GROUP BY segmento_horario, medio_unificado, pais
),

all_cost_averages AS (
    SELECT * FROM cost_averages_by_segment
    UNION ALL
    SELECT * FROM cost_averages_fallback
),

spots_with_estimated_costs AS (
    SELECT 
        s.*,
        COALESCE(
            -- Prioridad 1: Costo real si existe
            s.costo_usd,
            -- Prioridad 2: Promedio del canal específico
            ca1.costo_promedio_segmento,
            -- Prioridad 3: Promedio general del segmento
            ca2.costo_promedio_segmento
        ) AS costo_final_usd,
        
        CASE 
            WHEN s.costo_usd IS NOT NULL AND s.costo_usd > 0 THEN FALSE
            ELSE TRUE
        END AS es_costo_estimado,
        
        CASE 
            WHEN s.costo_usd IS NOT NULL AND s.costo_usd > 0 THEN 'COSTO_REAL'
            WHEN ca1.costo_promedio_segmento IS NOT NULL THEN 'ESTIMADO_CANAL'
            WHEN ca2.costo_promedio_segmento IS NOT NULL THEN 'ESTIMADO_SEGMENTO'
            ELSE 'SIN_ESTIMACION'
        END AS tipo_costo
        
    FROM {{ ref('int_unified_spots') }} s
    
    -- Join para promedio por canal específico
    LEFT JOIN cost_averages_by_segment ca1
        ON s.segmento_horario = ca1.segmento_horario
        AND s.medio_unificado = ca1.medio_unificado
        AND s.pais = ca1.pais
        AND s.canal_original = ca1.canal_original
    
    -- Join para promedio general del segmento
    LEFT JOIN cost_averages_fallback ca2
        ON s.segmento_horario = ca2.segmento_horario
        AND s.medio_unificado = ca2.medio_unificado
        AND s.pais = ca2.pais
)

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
    
    -- Costos
    costo_final_usd AS costo_usd,
    es_costo_estimado,
    tipo_costo,
    
    -- Métricas calculadas
    CASE 
        WHEN duracion_segundos > 0 THEN costo_final_usd / duracion_segundos
        ELSE NULL
    END AS costo_por_segundo,
    
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

FROM spots_with_estimated_costs