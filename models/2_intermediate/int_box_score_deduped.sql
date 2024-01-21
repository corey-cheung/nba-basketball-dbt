WITH box_score AS (

    SELECT * FROM {{ ref('stg_box_score') }}
),

deduped AS (

    SELECT
        box_score_id,
        game_id,
        player_id,
        team_id,
        pts,
        reb,
        ast,
        blk,
        stl,
        turnover,
        oreb,
        dreb,
        fg3_pct,
        fg3a,
        fg3m,
        fg_pct,
        fga,
        fgm,
        ft_pct,
        fta,
        ftm,
        min,
        pf,
        airbyte_id,
        airbyte_emitted_timestamp
    FROM box_score
    QUALIFY ROW_NUMBER() OVER (PARTITION BY box_score_id ORDER BY airbyte_emitted_timestamp DESC) = 1
)

SELECT *
FROM deduped
