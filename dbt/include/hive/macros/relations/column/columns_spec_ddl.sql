{% macro hive__assert_columns_equivalent(sql) %}

  {%- set user_defined_columns = model['columns'] -%}
  {%- if not user_defined_columns -%}
      {{ exceptions.raise_contract_error([], []) }}
  {%- endif -%}

  {%- set sql_file_provided_columns = get_column_schema_from_query(sql, config.get('sql_header', none)) -%}

  {%- set schema_file_provided_columns = get_column_schema_from_query(get_empty_schema_sql(user_defined_columns)) -%}

  {%- set sql_columns = format_columns(sql_file_provided_columns) -%}
  {%- set yaml_columns = format_columns(schema_file_provided_columns)  -%}

  {%- if sql_columns|length != yaml_columns|length -%}
    {%- do exceptions.raise_contract_error(yaml_columns, sql_columns) -%}
  {%- endif -%}

  {%- for sql_col in sql_columns -%}
    {%- set yaml_col = [] -%}

    {%- for this_col in yaml_columns -%}
      {%- if this_col['name'] | lower == sql_col['name'] | lower -%}
        {%- do yaml_col.append(this_col) -%}
        {%- break -%}
      {%- endif -%}
    {%- endfor -%}

    {%- if not yaml_col -%}
      {%- do exceptions.raise_contract_error(yaml_columns, sql_columns) -%}
    {%- endif -%}

    {%- set sql_type = sql_col['formatted'] | upper | trim -%}
    {%- set yaml_type = yaml_col[0]['formatted'] | upper | trim -%}


    {%- if sql_type != yaml_type -%}
      {%- do exceptions.raise_contract_error(yaml_columns, sql_columns) -%}
    {%- endif -%}

  {%- endfor -%}

{% endmacro %}
