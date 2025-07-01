
--- Data Quality Checks ---
-- Check for null patient_id in patients table
SELECT COUNT(*) AS null_patient_id_count
FROM patients
WHERE patient_id IS NULL;

-- Check for null diagnosis_date in diagnoses
SELECT COUNT(*) AS null_diag_date
FROM diagnoses
WHERE diagnosis_date IS NULL;

-- Check for duplicates
SELECT patient_id, COUNT(*) AS dup_count
FROM patients
GROUP BY patient_id
HAVING COUNT(*) > 1;

-- Referential Integrity Check
SELECT t.patient_id
FROM treatments t
LEFT JOIN patients p ON t.patient_id = p.patient_id
WHERE p.patient_id IS NULL;


-- Create flags to address the data quality issues for further analytics
with data_quality_flags AS
SELECT 
    p.patient_id,
    CASE WHEN d.icd_code IS NULL THEN 'Missing Diagnosis' ELSE NULL END AS diag_issue
FROM patients p
LEFT JOIN diagnoses d ON p.patient_id = d.patient_id
WHERE p.patient_id IS NOT NULL;

-- Check for valid dates
SELECT 
    pd.patient_id,
    pd.patient_diag_date,
    pts.first_treatment_date
FROM diagnoses pd
JOIN treatments pts
    ON pd.patient_id = pts.patient_id;
    where pd.patient_diag_date >  pts.first_treatment_date



---- PL/SQL Procedure for data validation ----
CREATE OR REPLACE procedure validate_queries()
LANGUAGE plpgsql
AS $$
DECLARE
    validation_id INT;
    source_query TEXT;
    target_query TEXT;
    source_result INT;
    target_result INT;
    validation_status TEXT;
BEGIN
    -- Cursor to loop through each validation query
    FOR validation_id, source_query, target_query IN
        SELECT id, source_query, target_query
        FROM validation_queries
    LOOP
        -- Execute source query and get result
        EXECUTE source_query INTO source_result;
        
        -- Execute target query and get result
        EXECUTE target_query INTO target_result;
        
        -- Compare results and determine status
        IF source_result = target_result THEN
            validation_status := 'PASS';
        ELSE
            validation_status := 'FAIL';
        END IF;
        
        -- Insert the validation result into log table
        INSERT INTO validation_log (validation_id, status)
        VALUES (validation_id, validation_status);
    END LOOP;
END;
$$;

