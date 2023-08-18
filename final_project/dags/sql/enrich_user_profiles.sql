TRUNCATE TABLE `{{ params.project_id }}.gold.user_profiles_enriched`
;


INSERT `{{ params.project_id }}.gold.user_profiles_enriched`(
    client_id,
    first_name,
    last_name,
    email,
    registration_date,
    state,
    birth_date,
    phone_number
)
SELECT
    c.client_id,
    u.first_name,
    u.last_name,
    c.email,
    c.registration_date,
    u.state,
    u.birth_date,
    u.phone_number
FROM `{{ params.project_id }}.silver.customers` c
LEFT JOIN `{{ params.project_id }}.silver.user_profiles` u ON c.email = u.email
;