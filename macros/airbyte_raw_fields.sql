{% macro airbyte_raw_fields() %}

    _airbyte_ab_id AS airbyte_id,
    _airbyte_emitted_at AT TIME ZONE 'UTC' AS airbyte_emitted_timestamp

{% endmacro %}
