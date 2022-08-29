-- Config to set as table and not view
{{
    config(
        materialized = 'table'
    )
}}

-- Table for Monte Carlo dash app
SELECT 
	{{ dbt_utils.surrogate_key([
		'"Team"',
		'"Opponent"',
		'"Real_Score"',
		'"Opponent_Real_Score"'
		]) }} AS sk_games,
	"Team",
	"Opponent",
	"Real_Score",
	"Opponent_Real_Score"
FROM {{ ref('nhl_game_view') }}