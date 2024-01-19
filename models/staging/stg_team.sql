WITH team AS (

    SELECT * FROM {{ source('nba_basketball', '_airbyte_raw_team') }}
)

, final AS (

    SELECT
        (_airbyte_data -> 'team_id')::INT AS team_id,
        (_airbyte_data -> 'team_name_abbreviation')::TEXT AS team_name_abbreviation,
        (_airbyte_data -> 'city')::TEXT AS city,
        (_airbyte_data -> 'conference')::TEXT AS conference,
        (_airbyte_data -> 'division')::TEXT AS division,
        (_airbyte_data -> 'team_full_name')::TEXT AS team_full_name,
        (_airbyte_data -> 'team_name')::TEXT AS team_name,
        _airbyte_ab_id,
        _airbyte_emitted_at
    FROM team
)

SELECT *
FROM final
