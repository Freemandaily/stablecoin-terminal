{% macro iceberg_table_exists(schema, identifier) %}
    {# SHOW TABLES uses the Spark session's Iceberg catalog directly; source()
       is avoided for the same reason as iceberg_relation(). Models fall back to
       an honest empty relation when a raw table has not been bridged yet. #}
    {% if execute %}
        {% set tables = run_query("show tables in " ~ schema).rows %}
        {% for row in tables %}
            {% if row[1] == identifier %}
                {% do return(true) %}
            {% endif %}
        {% endfor %}
        {% do return(false) %}
    {% else %}
        {% do return(true) %}
    {% endif %}
{% endmacro %}
