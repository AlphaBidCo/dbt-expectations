{%- test expect_table_row_count_to_equal(model,
                                            value,
                                            group_by=None,
                                            row_condition=None
                                            ) -%}
    {{ adapter.dispatch('test_expect_table_row_count_to_equal',
                        'dbt_expectations') (model,
                                                value,
                                                group_by,
                                                row_condition
                                                ) }}
{% endtest %}



{%- macro default__test_expect_table_row_count_to_equal(model,
                                                        value,
                                                        group_by,
                                                        row_condition
                                                        ) -%}

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
        {{ value }} as expected_row_count
    from base
    {{ group_by_clause }}
),

failures as (
    select *
    from aggregated
    where actual_row_count != expected_row_count
)

select *
from failures

{%- endmacro -%}
