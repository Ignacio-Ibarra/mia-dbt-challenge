-- macros/select_except_columns.sql
{% macro select_except(table, exclude=[]) %}
    {%- set all_columns = adapter.get_columns_in_relation(table) -%}
    {%- set selected_columns = [] -%}
    
    {%- for col in all_columns -%}
        {%- if col.name not in exclude -%}
            {%- do selected_columns.append(col.name) -%}
        {%- endif -%}
    {%- endfor -%}
    
    {{ selected_columns | join(', ') }}
{% endmacro %}