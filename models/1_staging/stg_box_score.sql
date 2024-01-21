WITH box_score AS (

    SELECT * FROM {{ source('nba_basketball', '_airbyte_raw_box_score') }}
),

final AS (

    SELECT
        (_airbyte_data ->> 'box_score_id')::INTEGER AS box_score_id,
        (_airbyte_data ->> 'game_id')::INTEGER AS game_id,
        (_airbyte_data ->> 'player_id')::INTEGER AS player_id,
        (_airbyte_data ->> 'team_id')::INTEGER AS team_id,
        (_airbyte_data ->> 'pts')::INTEGER AS pts,
        (_airbyte_data ->> 'reb')::INTEGER AS reb,
        (_airbyte_data ->> 'ast')::INTEGER AS ast,
        (_airbyte_data ->> 'blk')::INTEGER AS blk,
        (_airbyte_data ->> 'stl')::INTEGER AS stl,
        (_airbyte_data ->> 'turnover')::INTEGER AS turnover,
        (_airbyte_data ->> 'oreb')::INTEGER AS oreb,
        (_airbyte_data ->> 'dreb')::INTEGER AS dreb,
        (_airbyte_data ->> 'fg3_pct')::FLOAT AS fg3_pct,
        (_airbyte_data ->> 'fg3a')::INTEGER AS fg3a,
        (_airbyte_data ->> 'fg3m')::INTEGER AS fg3m,
        (_airbyte_data ->> 'fg_pct')::FLOAT AS fg_pct,
        (_airbyte_data ->> 'fga')::INTEGER AS fga,
        (_airbyte_data ->> 'fgm')::INTEGER AS fgm,
        (_airbyte_data ->> 'ft_pct')::FLOAT AS ft_pct,
        (_airbyte_data ->> 'fta')::INTEGER AS fta,
        (_airbyte_data ->> 'ftm')::INTEGER AS ftm,
        (_airbyte_data ->> 'min')::TEXT AS min,
        (_airbyte_data ->> 'pf')::INTEGER AS pf,
        {{ airbyte_raw_fields() }}
    FROM box_score
)

SELECT *
FROM final
