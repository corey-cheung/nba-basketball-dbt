WITH game AS (

    SELECT * FROM {{ ref('int_game_deduped') }}
),

team AS (

    SELECT * FROM {{ ref('team') }}
),

final AS (

    SELECT
        game.game_id,
        game.game_date,
        game.home_team_id,
        home_team.team_name_abbreviation AS home_team,
        game.home_team_score,
        game.visitor_team_id,
        visitor_team.team_name_abbreviation AS visitor_team,
        game.visitor_team_score,
        game.home_team_score > game.visitor_team_score AS home_team_win,
        game.season,
        game.post_season,
        game.status,
        game.airbyte_id,
        game.airbyte_emitted_timestamp
    FROM game
    JOIN team AS home_team ON home_team.team_id = game.home_team_id
    JOIN team AS visitor_team ON visitor_team.team_id = game.visitor_team_id
)

SELECT *
FROM final
