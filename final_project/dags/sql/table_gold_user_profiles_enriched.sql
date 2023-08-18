CREATE TABLE IF NOT EXISTS `de-07-yehor-holyk.gold.user_profiles_enriched`
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