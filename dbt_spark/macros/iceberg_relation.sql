{% macro iceberg_relation(schema, identifier, catalog='iceberg') -%}
    {# spark_catalog and iceberg both point at the same GCS Hadoop warehouse,
       so a two-level name avoids dbt-spark's "Cannot set database in spark!"
       while still resolving to the Iceberg table. #}
    {{ schema }}.{{ identifier }}
{%- endmacro %}
