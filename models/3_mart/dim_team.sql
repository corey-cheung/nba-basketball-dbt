WITH team AS (

    SELECT * FROM {{ ref('stg_team') }}
)

SELECT
    team_id,
    team_name_abbreviation,
    city,
    conference,
    division,
    team_name_full,
    airbyte_emitted_timestamp
FROM team
