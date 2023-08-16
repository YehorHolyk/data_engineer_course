CREATE TABLE IF NOT EXISTS `{{ params.project_id }}.gold.user_profiles_enriched`
(
    client_id           INT64 NOT NULL,
    first_name          STRING NOT NULL,
    last_name           STRING NOT NULL,
    email               STRING NOT NULL,
    registration_date   DATE NOT NULL,
    state               STRING NOT NULL,
    birth_date          DATE NOT NULL,
    phone_number        STRING NOT NULL
)
PARTITION BY
    registration_date
    OPTIONS (
        require_partition_filter = FALSE)
;


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