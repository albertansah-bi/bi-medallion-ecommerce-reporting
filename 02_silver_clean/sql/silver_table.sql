SELECT DISTINCT
    order_id,
    customer_id,
    CAST(order_date AS DATE) AS order_date,
    total_amount,
    order_status
INTO silver_orders
FROM bronze_orders
WHERE total_amount >= 0;
