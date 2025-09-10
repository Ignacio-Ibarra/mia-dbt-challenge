{% macro export_csv(table, basename) %}
COPY (
    SELECT * FROM {{ table }}
) TO '../resultado/{{ basename }}.csv' (
    FORMAT CSV,
    HEADER,
    DELIMITER ',',
    QUOTE '"',
    FORCE_QUOTE *
);
{% endmacro %}