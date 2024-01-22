WITH game AS (

    SELECT * FROM {{ ref('stg_game') }}
),

deduped AS ( -- airbyte connector for the duck db destination is append only

    SELECT
        game_id,
        game_date,
        home_team_id,
        home_team_score,
        visitor_team_id,
        visitor_team_score,
        season,
        post_season,
        status,
        airbyte_id,
        airbyte_emitted_timestamp
    FROM game
    WHERE status = 'Final' -- get completed games
    AND home_team_score <> 0
    AND visitor_team_score <> 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY airbyte_emitted_timestamp DESC) = 1
)

SELECT *
FROM deduped
