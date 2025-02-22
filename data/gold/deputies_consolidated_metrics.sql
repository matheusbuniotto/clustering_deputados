-- Active: 1740245760783@@127.0.0.1@3306

SELECT 
    deps.deputy_id,
    deps.name,
    deps.party,
    prop.proposition_count,
    ARRAY_VALUE(lower(prop.ementas)) AS propositions_list
FROM bronze_db.dim_deputies AS deps
LEFT JOIN bronze_db.fact_propositions AS prop ON prop.deputy_id = deps.deputy_id
GROUP BY ALL