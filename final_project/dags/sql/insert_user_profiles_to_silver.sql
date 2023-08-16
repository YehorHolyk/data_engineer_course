TRUNCATE TABLE `{{ params.project_id }}.silver.user_profiles`
;


INSERT `{{ params.project_id }}.silver.user_profiles` (
    email,
    first_name,
    last_name,
    state,
    birth_date,
    phone_number,
    _id,
    _logical_dt,
    _job_start_dt
)
SELECT
    email,
    SPLIT(TRIM(full_name), ' ')[OFFSET(0)] AS first_name,
    SPLIT(TRIM(full_name), ' ')[OFFSET(1)] AS last_name,
    state,
    birth_date,
    phone_number,
    GENERATE_UUID() AS _id,
    CAST('{{ dag_run.logical_date }}' AS TIMESTAMP) AS _logical_dt,
    CAST('{{ dag_run.start_date }}'AS TIMESTAMP) AS _job_start_dt
FROM user_profiles_jsonl
;