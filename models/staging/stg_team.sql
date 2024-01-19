select * from {{ source('nba_basketball', '_airbyte_raw_team') }}
