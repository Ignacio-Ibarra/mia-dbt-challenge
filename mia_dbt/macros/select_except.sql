-- macros/select_except_columns.sql
{% macro select_except(relation, exclude=None, alias=None) %}
    {%- if exclude is none -%}
        {%- set exclude = [] -%}
    {%- endif -%}

    {%- set cols = adapter.get_columns_in_relation(relation) -%}
    {%- set selected = [] -%}

    {%- for c in cols -%}
        {%- if c.name not in exclude -%}
            {%- if alias -%}
                {%- do selected.append(alias ~ '.' ~ c.name) -%}
            {%- else -%}
                {%- do selected.append(c.name) -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}

    {{ return(selected | join(', ')) }}
{% endmacro %}