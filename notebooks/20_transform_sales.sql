CREATE OR REPLACE VIEW ${catalog}.${schema}.sales_by_store AS
SELECT store_id, SUM(total_amount) AS store_sales
FROM ${catalog}.${schema}.sales_curated
GROUP BY store_id;
