-- Select all tied games for testing
SELECT 
    "Real_Score",
    "Opponent_Real_Score",
    ("Real_Score" - "Opponent_Real_Score") AS "Tie_Breaker"
FROM {{ ref('raw_game_facts') }}
WHERE ("Real_Score" - "Opponent_Real_Score") = 0
