-- macros/auto_normalize_string.sql
{% macro auto_normalize_string(input_string, reference_table, reference_column, threshold=0.8) %}
    {#- 
    Macro para normalización automática de strings usando fuzzy matching
    
    Parámetros:
    - input_string: string a normalizar
    - reference_table: tabla con strings de referencia
    - reference_column: columna con strings de referencia
    - threshold: umbral de similitud (0.0 a 1.0, default 0.8)
    
    Retorna:
    - String normalizado si encuentra match, sino el string original
    #}
    
    COALESCE(
        (
            SELECT {{ reference_column }}
            FROM {{ reference_table }}
            WHERE jaro_winkler_similarity(
                UPPER(TRIM({{ input_string }})), 
                UPPER(TRIM({{ reference_column }}))
            ) >= {{ threshold }}
            ORDER BY jaro_winkler_similarity(
                UPPER(TRIM({{ input_string }})), 
                UPPER(TRIM({{ reference_column }}))
            ) DESC
            LIMIT 1
        ),
        {{ input_string }}
    )
{% endmacro %}