-- Active: 1740245760783@@127.0.0.1@3306

SELECT 
    deps.deputy_id,
    deps.name,
    deps.party,
    exp.tipoDespesa AS expense_type,
    exp.tipoDocumento AS document_type,
    SUM(exp.valorLiquido) AS total_expenses,
    COUNT(*) AS total_documents
FROM deputies_db.bronze.dim_deputies AS deps
LEFT JOIN deputies_db.bronze.fact_expenses AS exp ON exp.deputy_id = deps.deputy_id
GROUP BY ALL
