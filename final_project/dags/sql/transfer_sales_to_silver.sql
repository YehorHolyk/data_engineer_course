DELETE FROM `{{ params.project_id }}.silver.sales`
WHERE DATE(_logical_dt) = "{{ ds }}"
;

INSERT `{{ params.project_id }}.silver.sales` (
    client_id,
    purchase_date,
    product_name,
    price,
    _id,
    _logical_dt,
    _job_start_dt
)
SELECT
    CAST(CustomerId AS INT64) as client_id,
    (CASE
        WHEN REGEXP_CONTAINS(PurchaseDate, r'\d{4}\/\d{2}\/\d{2}') THEN CAST(PurchaseDate AS DATE FORMAT 'YYYY/MM/DD')
        WHEN REGEXP_CONTAINS(PurchaseDate, r'\d{4}\-\d{2}\-\d{1,2}') THEN CAST(PurchaseDate AS DATE FORMAT 'YYYY-MM-DD')
        WHEN REGEXP_CONTAINS(PurchaseDate, r'\d{4}\-\w{3}\-\d{2}') THEN CAST(PurchaseDate AS DATE FORMAT 'YYYY-MON-DD')
        ELSE NULL END) AS purchase_date,
    Product,
    CAST(REPLACE(REPLACE(Price,'$',''),'USD','') AS INT64) as price,
    _id,
    _logical_dt,
    _job_start_dt
FROM `{{ params.project_id }}.bronze.sales`
;