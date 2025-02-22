WITH dep_table AS (
    SELECT
        d.deputy_id,
        d.name,
        d.party,
        dpc.party_idelogical_position AS party_classification,
        dpc.party_score_ideology_median AS party_score,
        d.state,
        d.photo_url,
        d.idLegislatura AS legislation_id
    FROM deputies_db.bronze.dim_deputies AS d
    LEFT JOIN deputies_db.silver.deputies_party_classification AS dpc 
        ON d.deputy_id = dpc.deputy_id
),

amount_spent AS (
    SELECT
        deputy_id,
        SUM(CASE WHEN expense_type = 'SERVIÇO DE TÁXI, PEDÁGIO E ESTACIONAMENTO' THEN total_expenses ELSE 0 END) AS taxi_toll_parking,
        SUM(CASE WHEN expense_type = 'PASSAGEM AÉREA - SIGEPA' THEN total_expenses ELSE 0 END) AS flight_passages,
        SUM(CASE WHEN expense_type = 'MANUTENÇÃO DE ESCRITÓRIO DE APOIO À ATIVIDADE PARLAMENTAR' THEN total_expenses ELSE 0 END) AS office_maintenance,
        SUM(CASE WHEN expense_type = 'COMBUSTÍVEIS E LUBRIFICANTES' THEN total_expenses ELSE 0 END) AS fuel_lubricants,
        SUM(CASE WHEN expense_type = 'SERVIÇO DE TÁXI, PEDÁGIO E ESTACIONAMENTO' THEN total_documents ELSE 0 END) AS taxi_toll_parking_count,
        SUM(CASE WHEN expense_type = 'PASSAGEM AÉREA - SIGEPA' THEN total_documents ELSE 0 END) AS flight_passages_count,
        SUM(CASE WHEN expense_type = 'MANUTENÇÃO DE ESCRITÓRIO DE APOIO À ATIVIDADE PARLAMENTAR' THEN total_documents ELSE 0 END) AS office_maintenance_count,
        SUM(CASE WHEN expense_type = 'COMBUSTÍVEIS E LUBRIFICANTES' THEN total_documents ELSE 0 END) AS fuel_lubricants_count,
        SUM(total_expenses) AS total_expenses,
        SUM(total_documents) AS total_documents

    FROM deputies_db.silver.deputies_expenses
    GROUP BY 1
),

attendance_table AS (
    SELECT 
        deputy_id,
        attendance_count,
        justified_absence_count,
        unjustified_absence_count,
        total_days,
        attendance_rate
        
    FROM deputies_db.silver.deputies_attendance
),

propositions AS (
    SELECT
        deputy_id,
        proposition_count,
        propositions_list
    FROM deputies_db.silver.deputies_propositions
),

tb_final AS (
    SELECT DISTINCT 
        dt.deputy_id,
        dt.name,
        dt.party,
        dt.party_classification,
        dt.party_score,
        dt.state,
        dt.photo_url,
        dt.legislation_id,
        at.attendance_count,
        at.justified_absence_count,
        at.unjustified_absence_count,
        at.total_days,
        at.attendance_rate,
        am.taxi_toll_parking,
        am.flight_passages,
        am.office_maintenance,
        am.fuel_lubricants,
        am.total_expenses,
        am.taxi_toll_parking_count,
        am.flight_passages_count,
        am.office_maintenance_count,
        am.fuel_lubricants_count,
        am.total_documents,
        p.proposition_count,
        p.propositions_list
    FROM dep_table AS dt
    LEFT JOIN amount_spent AS am ON dt.deputy_id = am.deputy_id
    LEFT JOIN attendance_table AS at ON dt.deputy_id = at.deputy_id
    LEFT JOIN propositions AS p ON dt.deputy_id = p.deputy_id
)
SELECT * FROM tb_final;