-- Config to set as table and not view
{{
    config(
        materialized = 'table'
    )
}}

SELECT * 
-- FROM nhlgamesdb.raw.teams
FROM {{ source('raw', 'opponents') }}

{# SELECT * 
FROM nhlgamesdb.raw.teams #}