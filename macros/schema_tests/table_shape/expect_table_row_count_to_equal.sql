{%- test expect_table_row_count_to_equal(model,
                                            value,
                                            group_by=None,
                                            row_condition=None,
                                            severity='error',
                                            warn_if=None,
                                            error_if=None,
                                            fail_calc='actual_row_count'
                                            ) -%}
    {{ adapter.dispatch('test_expect_table_row_count_to_equal',
                        'dbt_expectations') (
                            model,
                            value,
                            group_by,
                            row_condition,
                            severity,
                            warn_if,
                            error_if,
                            fail_calc
                        ) }}
{%- endtest %}



{%- macro default__test_expect_table_row_count_to_equal(model,
                                                        value,
                                                        group_by,
                                                        row_condition,
                                                        severity,
                                                        warn_if,
                                                        error_if,
                                                        fail_calc
                                                        ) -%}

{{ config(
    severity=severity,
    fail_calc=fail_calc,
    warn_if=warn_if or '',
    error_if=error_if or ''
) }}

{%- if value is none or value | string | trim == '' -%}
    {% do exceptions.raise("❌ Missing required `value:` argument in `expect_table_row_count_to_equal`.") %}
{%- endif -%}

{%- set where_clause -%}
    {%- if row_condition -%}
        where {{ row_condition }}
    {%- endif -%}
{%- endset -%}

{%- set group_by_clause -%}
    {%- if group_by -%}
        group by {{ group_by | join(", ") }}
    {%- endif -%}
{%- endset -%}

with base as (
    select *
    from {{ model }}
    {{ where_clause }}
),

aggregated as (
    select
        count(*) as actual_row_count,
        {{ value }} as expected_row_count,
        (count(*) != {{ value }}) as test_failed
    from base
    {{ group_by_clause }}
)

select *
from aggregated
{%- endmacro -%}
