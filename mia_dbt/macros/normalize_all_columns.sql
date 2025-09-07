-- macros/normalize_all_columns.sql
{% macro normalize_all_columns(table_ref) %}
    {#- Obtener metadata de la tabla #}
    {% set cols = adapter.get_columns_in_relation(ref(table_ref)) %}

    {%- set col_expressions = [] -%}
    {% for col in cols %}
        {%- set normalized_name = col.name | lower | replace(' ', '_') | replace('-', '_') | replace('.', '_') | regex_replace('[^a-z0-9_]', '') -%}
        {%- do col_expressions.append(col.name ~ ' as ' ~ normalized_name) -%}
    {% endfor %}

    {{ return(col_expressions | join(',\n    ')) }}
{% endmacro %}