-- Active: 1740245760783@@127.0.0.1@3306

SELECT 
    deps.deputy_id,
    deps.name,
    deps.party,
    at.presencas AS attendace_count,
    at.ausencias_justificadas AS justified_absence_count,
    at.ausencias_nao_justificadas AS unjustified_absence_count,
    at.total_dias AS total_days,
    at.taxa_presenca AS attendance_rate,
    at.year
FROM deputies_db.bronze.dim_deputies AS deps
LEFT JOIN deputies_db.bronze.fact_attendace AS at ON at.deputy_id = deps.deputy_id
GROUP BY ALL