WITH game AS (

    SELECT * FROM {{ ref('stg_game') }}
),

deduped AS (

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
    QUALIFY ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY airbyte_emitted_timestamp DESC) = 1
)

SELECT *
FROM deduped
