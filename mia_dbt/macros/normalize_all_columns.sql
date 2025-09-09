-- macros/normalize_all_columns.sql
{% macro normalize_all_columns(relation) %}
    {% set cols = adapter.get_columns_in_relation(relation) %}
    {% set col_expressions = [] %}

    {% for col in cols %}
        {# Paso 1: minúsculas y underscores #}
        {% set normalized_name = col.name | lower
            | replace('. ', '_')
            | replace(' ', '_')
            | replace('-', '_')
            | replace('.', '_')
            | replace('/', '_')
            
        %}

        {# Paso 2: quitar tildes manualmente (fallback si no hay unidecode) #}
        {% set normalized_name = normalized_name
            | replace('á','a') 
            | replace('é','e') 
            | replace('í','i') 
            | replace('ó','o') 
            | replace('ú','u')
            | replace('ñ','n')
        %}

        {# Paso 3: limpiar caracteres raros #}
        {% set normalized_name = modules.re.sub('[^a-z0-9_]', '', normalized_name) %}

        {% do col_expressions.append('"'~col.name~'" as ' ~ normalized_name) %}
    {% endfor %}

    {{ return(col_expressions | join(',\n    ')) }}
{% endmacro %}
