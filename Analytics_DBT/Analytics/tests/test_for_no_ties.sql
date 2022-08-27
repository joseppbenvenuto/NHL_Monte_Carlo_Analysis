-- Select all tied games for testing
SELECT 
    "GF", 
    "GA", 
    ("GF" - "GA") AS "Tie_Breaker"
FROM {{ ref('raw_game_facts') }}
WHERE ("GF" - "GA") = 0
