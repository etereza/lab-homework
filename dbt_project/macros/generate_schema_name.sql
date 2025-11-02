{% macro generate_schema_name(custom_schema_name, node) -%}
    {# Схема за замовчуванням = target.schema #}
    {% set default_schema = target.schema %}

    {% if custom_schema_name is none %}
        {{ default_schema }}
    {% else %}
        {{ custom_schema_name | trim }}
    {% endif %}
{%- endmacro %}