-- Active: 1740245760783@@127.0.0.1@3306

SELECT 
    deps.deputy_id,
    deps.name,
    deps.party,
    parties.posicao_ideologica AS party_idelogical_position,
    parties.mediana AS party_score_ideology_median
FROM deputies_db.bronze.dim_deputies AS deps
LEFT JOIN deputies_db.bronze.partidos_classificados AS parties ON LOWER(parties.partido) = LOWER(deps.party)
GROUP BY ALL