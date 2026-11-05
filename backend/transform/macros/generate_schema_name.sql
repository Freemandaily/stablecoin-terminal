{# Use custom schema names exactly as written (staging, marts) instead of dbt's
   default `<target.schema>_<custom>` (which would yield public_staging/public_marts).
   Models without a +schema fall back to target.schema (public). — B-D17 #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
