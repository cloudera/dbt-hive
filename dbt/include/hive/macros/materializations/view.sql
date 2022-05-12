{% materialization view, adapter='hive' -%}
    {{ return(create_or_replace_view()) }}
{%- endmaterialization %}
