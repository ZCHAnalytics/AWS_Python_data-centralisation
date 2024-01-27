/* Task 6: Query the databse to find which months in which years have had the most sales historically. */

SELECT 
	ROUND(CAST(SUM(orders_table.product_quantity * dim_products.product_price) AS NUMERIC), 2) AS total_sales,
	"year",
	"month"
FROM orders_table
LEFT JOIN dim_products ON orders_table.product_code = dim_products.product_code
LEFT JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY "year", "month"
ORDER BY total_sales DESC;
