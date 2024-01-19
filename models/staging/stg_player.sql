WITH player AS (

    SELECT * FROM {{ source('nba_basketball', '_airbyte_raw_player') }}
),

final AS (

    SELECT
        (_airbyte_data -> 'player_id')::INTEGER AS player_id,
        (_airbyte_data -> 'position')::TEXT AS position,
        (_airbyte_data -> 'team_id')::INTEGER AS team_id,
        (_airbyte_data -> 'first_name')::TEXT AS first_name,
        (_airbyte_data -> 'last_name')::TEXT AS last_name,
        (_airbyte_data -> 'height_feet')::INTEGER AS height_feet,
        (_airbyte_data -> 'height_inches')::INTEGER AS height_inches,
        (_airbyte_data -> 'weight_pounds')::INTEGER AS weight_pounds,
        _airbyte_ab_id,
        _airbyte_emitted_at
    FROM player
)

SELECT *
FROM final
