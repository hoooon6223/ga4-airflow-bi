{#
  dbt's default would name the staging dataset "<target>_ga4_staging".
  Use the custom schema as-is instead, so staging always lands in ga4_staging
  and marts land in the target's dataset (ga4_mart or ga4_mart_test).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
