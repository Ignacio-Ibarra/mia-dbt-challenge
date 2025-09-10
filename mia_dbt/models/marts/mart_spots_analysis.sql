-- mia_dbt/models/marts/mart_spots_analysis.sql
{{
  config(
    materialized='table',
    post_hook = [
        "{{ export_csv(this, 'mart_spots_analysis') }}"
    ]
) }}

/*
    Tabla final de negocio para análisis de spots de TV y Radio
    
    Propósito:
    - Tabla unificada y normalizada para análisis de negocio
    - Integra datos de Brasil y México con costos estimados
    - Incluye normalizaciones de marcas y canales
    - Métricas calculadas para análisis de performance
    
    Fuentes:
    - int_unified_spots: Datos unificados de ambos países
    - int_cost_estimation: Costos estimados por segmento horario
    - int_fuzzy_brand_matching: Normalizaciones de marcas
    - int_fuzzy_channel_matching: Normalizaciones de canales
*/

WITH spots_with_costs AS (
    SELECT 
        s.*,
        ce.costo_usd as costo_final_usd,
        ce.tipo_costo,
        ce.es_costo_estimado
    FROM {{ ref('int_unified_spots') }} s
    LEFT JOIN {{ ref('int_cost_estimation') }} ce 
        ON s.spot_id = ce.spot_id
),

-- Aplicar normalizaciones de marcas
spots_with_brand_normalization AS (
    SELECT 
        s.*,
        COALESCE(bm.marca_sugerida, s.marca_original) as marca_normalizada,
        bm.confidence_level as marca_confidence,
        bm.recomendacion as marca_recomendacion
    FROM spots_with_costs s
    LEFT JOIN {{ ref('int_fuzzy_brand_matching') }} bm 
        ON s.marca_original = bm.marca_original
        AND s.pais = bm.pais
),

-- Aplicar normalizaciones de canales
spots_with_channel_normalization AS (
    SELECT 
        s.*,
        COALESCE(cm.canal_sugerido, s.canal_original) as canal_normalizado,
        cm.confidence_level as canal_confidence,
        cm.recomendacion as canal_recomendacion
    FROM spots_with_brand_normalization s
    LEFT JOIN {{ ref('int_fuzzy_channel_matching') }} cm 
        ON s.canal_original = cm.canal_original
        AND s.pais = cm.pais
),

-- Calcular métricas adicionales
spots_with_metrics AS (
    SELECT 
        *,
        -- Métricas de costo por duración
        CASE 
            WHEN duracion_segundos > 0 THEN costo_final_usd / duracion_segundos
            ELSE NULL
        END as costo_por_segundo,
        
        -- Métricas de costo por cobertura
        CASE 
            WHEN TRY_CAST(cobertura AS DOUBLE) > 0 THEN costo_final_usd / TRY_CAST(cobertura AS DOUBLE)
            ELSE NULL
        END as costo_por_cobertura,
        
        -- Clasificación de spot por duración
        CASE 
            WHEN duracion_segundos <= 15 THEN 'CORTO'
            WHEN duracion_segundos <= 30 THEN 'MEDIO'
            WHEN duracion_segundos <= 60 THEN 'LARGO'
            ELSE 'MUY_LARGO'
        END as clasificacion_duracion,
        
        -- Clasificación de spot por costo
        CASE 
            WHEN costo_final_usd <= 100 THEN 'BAJO'
            WHEN costo_final_usd <= 500 THEN 'MEDIO'
            WHEN costo_final_usd <= 1000 THEN 'ALTO'
            ELSE 'MUY_ALTO'
        END as clasificacion_costo,
        
        -- Indicador de calidad de datos
        CASE 
            WHEN tipo_costo = 'REAL' AND marca_confidence = 'high' AND canal_confidence = 'high' THEN 'ALTA'
            WHEN tipo_costo = 'ESTIMADO' OR marca_confidence IN ('medium', 'low') OR canal_confidence IN ('medium', 'low') THEN 'MEDIA'
            ELSE 'BAJA'
        END as calidad_datos
        
    FROM spots_with_channel_normalization
)

SELECT 
    -- Identificadores
    spot_id,
    pais,
    
    -- Dimensiones temporales
    fecha,
    hora,
    hora_numero,
    segmento_horario,
    
    -- Dimensiones de medio y canal
    medio_unificado,
    medio_original,
    canal_original,
    canal_normalizado,
    canal_confidence,
    canal_recomendacion,
    estacion_canal,
    
    -- Dimensiones de marca y producto
    marca_original,
    marca_normalizada,
    marca_confidence,
    marca_recomendacion,
    producto,
    version_original,
    
    -- Métricas de duración y costo
    duracion_segundos,
    clasificacion_duracion,
    costo_final_usd,
    tipo_costo,
    clasificacion_costo,
    costo_por_segundo,
    costo_por_cobertura,
    
    -- Métricas de cobertura y alcance
    cobertura,
    spot_tipo,
    
    -- Dimensiones de categorización
    sector,
    subsector,
    categoria,
    
    -- Metadatos de calidad
    calidad_datos,
    fecha_file,
    fuente_pais,
    
    -- Columnas específicas por país (mantener para referencia)
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
    esprimera

FROM spots_with_metrics
ORDER BY 
    pais,
    fecha,
    hora,
    medio_unificado,
    canal_normalizado,
    marca_normalizada