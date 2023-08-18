DELETE FROM `{{ params.project_id }}.silver.customers`
WHERE DATE(_logical_dt) = "{{ ds }}"
;

MERGE `{{ params.project_id }}.silver.customers` T
USING (
        WITH CTE AS(
            SELECT
            Id,
            FirstName,
            LastName,
            Email,
            RegistrationDate,
            State,
            _id,
            _logical_dt,
            _job_start_dt,
            ROW_NUMBER() OVER(PARTITION BY Id ORDER BY Id ASC) AS RN
            FROM
            `{{ params.project_id }}.bronze.customers`
            WHERE DATE(_logical_dt) = "{{ ds }}"
        )
        SELECT
            Id,
            FirstName,
            LastName,
            Email,
            RegistrationDate,
            State,
            _id,
            _logical_dt,
            _job_start_dt
        FROM CTE WHERE RN=1
    ) S
ON
    T.client_id = CAST(S.Id AS INT64) AND
    T.first_name = S.FirstName AND
    T.last_name = S.LastName AND
    T.email = S.Email AND
    T.registration_date = CAST(S.RegistrationDate AS DATE FORMAT 'YYYY-MM-DD') AND
    T.state = S.State
WHEN NOT MATCHED THEN
    INSERT(
           client_id,
           first_name,
           last_name,
           email,
           registration_date,
           state,
           _id,
           _logical_dt,
           _job_start_dt
           )
    VALUES(
           CAST(Id AS INT64),
           FirstName,
           LastName,
           Email,
           CAST(S.RegistrationDate AS DATE FORMAT 'YYYY-MM-DD'),
           State,
           _id,
           _logical_dt,
           _job_start_dt
           )
WHEN MATCHED THEN
    UPDATE SET
    T.client_id = CAST(S.Id AS INT64),
    T.first_name = S.FirstName,
    T.last_name = S.LastName,
    T.email = S.Email,
    T.registration_date = CAST(S.RegistrationDate AS DATE FORMAT 'YYYY-MM-DD'),
    T.state = S.State,
    T._id = S._id,
    T._logical_dt = S._logical_dt,
    T._job_start_dt =  S._job_start_dt
;