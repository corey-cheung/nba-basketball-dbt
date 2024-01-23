WITH box_score AS (

    SELECT * FROM {{ ref('fct_box_score') }}
),

season_stats AS (

    SELECT
        MD5(CONCAT(player_id, season)) AS season_stats_id,
        player_id,
        first_name,
        last_name,
        season,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY season) AS season_number,
        SUM(pts) AS total_pts,
        SUM(reb) AS total_reb,
        SUM(ast) AS total_ast,
        SUM(blk) AS total_blk,
        SUM(stl) AS total_stl,
        COUNT(DISTINCT game_id) AS total_games,
        SUM(turnover) AS total_turnover,
        AVG(pts) AS avg_pts,
        AVG(reb) AS avg_reb,
        AVG(ast) AS avg_ast,
        AVG(blk) AS avg_blk,
        AVG(stl) AS avg_stl,
        AVG(turnover) AS avg_turnover,
        AVG(fg3_pct) AS avg_fg3_pct,
        AVG(fg_pct) AS avg_fg_pct,
        AVG(ft_pct) AS avg_ft_pct,
        SUM(total_pts) OVER (PARTITION BY player_id ORDER BY season) AS career_pts,
        SUM(total_reb) OVER (PARTITION BY player_id ORDER BY season) AS career_reb,
        SUM(total_ast) OVER (PARTITION BY player_id ORDER BY season) AS career_ast,
        SUM(total_blk) OVER (PARTITION BY player_id ORDER BY season) AS career_blk,
        SUM(total_stl) OVER (PARTITION BY player_id ORDER BY season) AS career_stl,
        SUM(total_turnover) OVER (PARTITION BY player_id ORDER BY season) AS career_turnover
    FROM box_score
    WHERE NOT post_season
    GROUP BY season_stats_id, player_id, first_name, last_name, season
)

SELECT *
FROM season_stats
