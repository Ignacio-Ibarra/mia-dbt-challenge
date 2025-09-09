-- mia_dbt/models/intermediate/int_fuzzy_channel_matching.sql
{{ config(materialized='view') }}

/*
    Modelo para fuzzy matching automático de canales
    
    Propósito:
    - Identificar canales similares usando algoritmos de similitud
    - Crear mapeo automático entre variaciones de nombres de canales
    - Generar sugerencias para el diccionario de canales
    
    Fuente:
    - int_unified_spots: Datos unificados para extraer canales únicos
    - channel_dictionary: Diccionario de canales existente
*/

WITH unique_channels AS (
    SELECT DISTINCT 
        canal_original as channel_name,
        pais
    FROM {{ ref('int_unified_spots') }}
    WHERE canal_original IS NOT NULL
),

-- Calcular similitudes con todos los umbrales
similarity_scores AS (
    SELECT 
        i.channel_name as input_string,
        r.canal_normalizado as reference_string,
        jaro_winkler_similarity(
            UPPER(TRIM(i.channel_name)), 
            UPPER(TRIM(r.canal_normalizado))
        ) as similarity_score,
        i.pais
    FROM unique_channels i
    CROSS JOIN {{ ref('channel_dictionary') }} r
),

-- Encontrar el mejor match para cada canal
best_matches AS (
    SELECT 
        input_string,
        reference_string,
        similarity_score,
        pais,
        ROW_NUMBER() OVER (
            PARTITION BY input_string 
            ORDER BY similarity_score DESC
        ) as rank
    FROM similarity_scores
),

-- Clasificar por nivel de confianza
classified_matches AS (
    SELECT 
        input_string as canal_original,
        reference_string as canal_sugerido,
        similarity_score,
        pais,
        CASE 
            WHEN similarity_score = 1.0 THEN 'exact'
            WHEN similarity_score >= 0.9 THEN 'fuzzy'
            WHEN similarity_score >= 0.7 THEN 'fuzzy'
            WHEN similarity_score >= 0.5 THEN 'fuzzy'
            ELSE 'no_match'
        END as match_type,
        CASE 
            WHEN similarity_score >= 0.9 THEN 'high'
            WHEN similarity_score >= 0.7 THEN 'medium'
            WHEN similarity_score >= 0.5 THEN 'low'
            ELSE 'none'
        END as confidence_level
    FROM best_matches
    WHERE rank = 1
),

-- Agregar recomendaciones
final_matches AS (
    SELECT 
        canal_original,
        canal_sugerido,
        similarity_score,
        match_type,
        confidence_level,
        pais,
        CASE 
            WHEN match_type = 'exact' THEN 'Aplicar automáticamente'
            WHEN confidence_level = 'high' THEN 'Revisar y aplicar'
            WHEN confidence_level = 'medium' THEN 'Revisar manualmente'
            WHEN confidence_level = 'low' THEN 'Revisar cuidadosamente'
            ELSE 'No aplicar'
        END as recomendacion
    FROM classified_matches
    WHERE match_type != 'no_match'
)

SELECT 
    canal_original,
    canal_sugerido,
    similarity_score,
    match_type,
    confidence_level,
    pais,
    recomendacion
FROM final_matches
ORDER BY 
    pais,
    confidence_level DESC,
    similarity_score DESC