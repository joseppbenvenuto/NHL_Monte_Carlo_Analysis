-- Select all tied games for testing
SELECT 
    real_score,
    opponent_real_score,
     {{ tie_breaker('real_score','opponent_real_score') }} AS tie_breaker
FROM {{ ref('raw_game_facts') }}
WHERE {{ tie_breaker('real_score','opponent_real_score') }} = 0
