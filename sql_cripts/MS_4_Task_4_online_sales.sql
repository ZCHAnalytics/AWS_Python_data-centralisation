/* TASK 4: Query the databse to find online sales vs offline.
Calculate how many products were sold and the amount of sales made for online and offline purchases.*/

SELECT 
	ROUND(COUNT(orders_table.product_code)) AS number_of_sales,
	ROUND(SUM(orders_table.product_quantity)) AS product_quantity_count, 
	CASE 
		WHEN dim_store_details.store_type = 'Web Portal' 
		THEN 'Web' 
		ELSE 'Offline' 
		END AS location
FROM orders_table
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY location
ORDER BY number_of_sales, product_quantity_count; 
