WITH box_score AS (

    SELECT * FROM {{ ref('int_box_score_deduped') }}
),

game AS (

    SELECT * FROM {{ ref('fct_game') }}
),

player AS (

    SELECT * FROM {{ ref('dim_player') }}
),

team AS (

    SELECT * FROM {{ ref('dim_team') }}
),

final AS (

    SELECT
        box_score.box_score_id,
        box_score.game_id,
        game.game_date,
        game.season,
        game.post_season,
        game.status,
        game.home_team_win,
        box_score.player_id,
        player.position,
        player.first_name,
        player.last_name,
        box_score.team_id,
        team.team_name_abbreviation,
        box_score.team_id = game.home_team_id AS home_team,
        box_score.pts,
        box_score.reb,
        box_score.ast,
        box_score.blk,
        box_score.stl,
        box_score.turnover,
        box_score.oreb,
        box_score.dreb,
        box_score.fg3_pct,
        box_score.fg3a,
        box_score.fg3m,
        box_score.fg_pct,
        box_score.fga,
        box_score.fgm,
        box_score.ft_pct,
        box_score.fta,
        box_score.ftm,
        box_score.min,
        box_score.pf,
        box_score.airbyte_id,
        box_score.airbyte_emitted_timestamp
    FROM box_score
    JOIN game USING(game_id)
    LEFT JOIN player USING(player_id)
    LEFT JOIN team ON team.team_id = box_score.team_id
)

SELECT *
FROM final
