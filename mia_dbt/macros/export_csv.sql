{% macro export_csv(table, basename) %}
COPY (
    SELECT * FROM {{ table }}
) TO '{{ env_var('EXPORT_PATH') }}/{{ basename }}.csv' (
    FORMAT CSV,
    HEADER,
    DELIMITER ',',
    QUOTE '"',
    FORCE_QUOTE *
);
{% endmacro %}