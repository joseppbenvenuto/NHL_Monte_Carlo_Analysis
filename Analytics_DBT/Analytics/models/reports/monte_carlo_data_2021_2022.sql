-- Config to set as table and not view
{{
    config(
        materialized = 'table'
    )
}}

-- Table for Monte Carlo dash app
SELECT 
	{{ dbt_utils.surrogate_key([
		'date',
		'team',
		'opponent',
		'real_score',
		'opponent_real_score'
		]) }} AS sk_games,
	TO_DATE(date, 'YYYY/MM/DD') AS date,
	team,
	opponent,
	real_score,
	opponent_real_score
FROM {{ ref('nhl_game_view') }}
WHERE date > '2021-05-19' AND date <= '2022-05-01'