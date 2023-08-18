SELECT up.state, COUNT(1) AS sales_count
FROM `de-07-yehor-holyk.gold.user_profiles_enriched` up
INNER JOIN `de-07-yehor-holyk.silver.sales` s ON up.client_id=s.client_id
WHERE
  DATE_DIFF(s.purchase_date, up.birth_date, year) BETWEEN 20 AND 30
  AND s.product_name = 'TV'
  AND s.purchase_date BETWEEN '2022-09-01' AND '2022-09-10'
GROUP BY up.state
ORDER BY COUNT(1) DESC
LIMIT 1
;