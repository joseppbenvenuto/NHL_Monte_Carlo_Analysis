-- Config to set as table and not view
{{
    config(
        materialized = 'table'
    )
}}

SELECT * 
-- FROM nhlgamesdb.raw.teams
FROM {{ source('raw', 'game_facts') }}

{# SELECT * 
FROM nhlgamesdb.raw.teams #}