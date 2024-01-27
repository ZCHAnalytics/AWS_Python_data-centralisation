/* Task 5: Query databse to find total and percentage of sales coming from each of the different store types. */

SELECT store_type, 
    ROUND(CAST(SUM(orders_table.product_quantity * dim_products.product_price) AS NUMERIC), 2) AS total_sales,
	ROUND(CAST(COUNT(orders_table.date_uuid) AS NUMERIC)/ 120123 * 100, 2) AS "percentage_total(%)"

FROM orders_table
LEFT JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code -- for orders per store 

LEFT JOIN dim_products ON orders_table.product_code = dim_products.product_code -- for sales per store 
GROUP BY store_type
ORDER BY "percentage_total(%)" DESC, total_sales, store_type;