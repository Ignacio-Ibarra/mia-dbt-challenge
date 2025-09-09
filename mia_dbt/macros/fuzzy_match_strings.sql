-- macros/fuzzy_match_strings.sql
{% macro fuzzy_match_strings(input_table, input_column, reference_table, reference_column, threshold=0.8) %}
    {#- 
    Macro para hacer fuzzy matching entre dos columnas de texto
    
    Parámetros:
    - input_table: tabla con los strings a matchear
    - input_column: columna con los strings de entrada
    - reference_table: tabla con los strings de referencia
    - reference_column: columna con los strings de referencia
    - threshold: umbral de similitud (0.0 a 1.0, default 0.8)
    
    Retorna:
    - input_string: string original
    - matched_string: string más similar encontrado
    - similarity_score: puntaje de similitud
    - match_type: 'exact', 'fuzzy', o 'no_match'
    #}
    
    WITH similarity_scores AS (
        SELECT 
            i.{{ input_column }} as input_string,
            r.{{ reference_column }} as reference_string,
            -- Usar Jaro-Winkler similarity (más rápido que Levenshtein)
            jaro_winkler_similarity(
                UPPER(TRIM(i.{{ input_column }})), 
                UPPER(TRIM(r.{{ reference_column }}))
            ) as similarity_score
        FROM ({{ input_table }}) i
        CROSS JOIN {{ reference_table }} r
    ),
    
    best_matches AS (
        SELECT 
            input_string,
            reference_string,
            similarity_score,
            ROW_NUMBER() OVER (
                PARTITION BY input_string 
                ORDER BY similarity_score DESC
            ) as rank
        FROM similarity_scores
        WHERE similarity_score >= {{ threshold }}
    )
    
    SELECT 
        input_string,
        reference_string as matched_string,
        similarity_score,
        CASE 
            WHEN similarity_score = 1.0 THEN 'exact'
            WHEN similarity_score >= {{ threshold }} THEN 'fuzzy'
            ELSE 'no_match'
        END as match_type
    FROM best_matches
    WHERE rank = 1
    
{% endmacro %}