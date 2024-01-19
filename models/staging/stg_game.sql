WITH game AS (

    SELECT * FROM {{ source('nba_basketball', '_airbyte_raw_game') }}
),

final AS (

    SELECT
        (_airbyte_data -> 'game_id')::INTEGER AS game_id,
        (_airbyte_data -> 'game_date')::DATE AS game_date,
        (_airbyte_data -> 'home_team_id')::INTEGER AS home_team_id,
        (_airbyte_data -> 'home_team_score')::INTEGER AS home_team_score,
        (_airbyte_data -> 'visitor_team_id')::INTEGER AS visitor_team_id,
        (_airbyte_data -> 'visitor_team_score')::INTEGER AS visitor_team_score,
        (_airbyte_data -> 'season')::INTEGER AS season,
        (_airbyte_data -> 'post_season')::BOOLEAN AS post_season,
        (_airbyte_data -> 'status')::TEXT AS status,
        _airbyte_ab_id,
        _airbyte_emitted_at
    FROM game
)

SELECT *
FROM final
