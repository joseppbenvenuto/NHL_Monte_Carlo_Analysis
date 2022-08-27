-- Config to set as table and not view
{{
    config(
        materialized = 'table'
    )
}}

SELECT * 
-- FROM nhlgamesdb.raw.teams
FROM {{ source('raw', 'teams') }}

{# SELECT * 
FROM nhlgamesdb.raw.teams #}