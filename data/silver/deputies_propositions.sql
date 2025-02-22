-- Active: 1740245760783@@127.0.0.1@3306

SELECT 
    deps.deputy_id,
    deps.name,
    deps.party,
    prop.proposition_count,
    ARRAY_VALUE(lower(prop.ementas)) AS propositions_list
FROM deputies_db.bronze.dim_deputies AS deps
LEFT JOIN deputies_db.bronze.fact_propositions AS prop ON prop.deputy_id = deps.deputy_id
GROUP BY ALL