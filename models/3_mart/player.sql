WITH player AS (

    SELECT * FROM {{ ref('stg_player') }}
),

team AS (

    SELECT * FROM {{ ref('team') }}
),

final AS (
    SELECT
        player.player_id,
        player.position,
        player.team_id,
        player.first_name,
        player.last_name,
        player.height_feet,
        player.height_inches,
        player.weight_pounds,
        team.team_name_abbreviation,
        player.airbyte_emitted_timestamp
    FROM player
    LEFT JOIN team USING(team_id)
)

SELECT *
FROM final
