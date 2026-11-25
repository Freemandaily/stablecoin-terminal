{# Keep custom schema names exact (staging, marts) so Spark outputs land in
   marts.* — the same contract the Postgres API queries. — B-D17 #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
