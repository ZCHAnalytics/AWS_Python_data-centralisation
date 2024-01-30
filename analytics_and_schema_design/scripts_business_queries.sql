
-- Business Inquiries

/* TASK 1: Query the database to find which countries we currently operate in and which now has the most stores. */

SELECT country_code, COUNT(store_code) AS total_no_stores FROM dim_store_details
GROUP by country_code 
ORDER BY total_no_stores DESC;


/* TASK 2: Query the database to find which locations have the most stores currently */

SELECT locality, COUNT(store_code) AS stores_total from dim_store_details
GROUP BY locality
ORDER BY stores_total DESC, locality ASC LIMIT 7;


/* TASK 3: Query the database to find out which months have produced the most sales */
SELECT
    ROUND(CAST(SUM(orders_table.product_quantity * dim_products.product_price)AS NUMERIC), 2) AS total_sales,
	dim_date_times.month
FROM orders_table
JOIN 
	dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN
	dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY dim_date_times.month
ORDER BY total_sales DESC;

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


/* Task 5: Query databse to find total and percentage of sales coming from each of the different store types. */
SELECT store_type, 
    ROUND(CAST(SUM(orders_table.product_quantity * dim_products.product_price) AS NUMERIC), 2) AS total_sales,
	ROUND(CAST(COUNT(orders_table.date_uuid) AS NUMERIC)/ 120123 * 100, 2) AS "percentage_total(%)"

FROM orders_table
LEFT JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code -- for orders per store 

LEFT JOIN dim_products ON orders_table.product_code = dim_products.product_code -- for sales per store 
GROUP BY store_type
ORDER BY "percentage_total(%)" DESC, total_sales, store_type;


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

/* Task 6: Query database to find the overall staff numbers in each location around the world.  */
SELECT SUM(staff_numbers) AS total_staff_number, country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_number DESC;


/* TASK 8: Query the database to find type of store that generates the most sales in Germany. */

SELECT
	ROUND(CAST(SUM(product_quantity * product_price) AS NUMERIC), 2) as total_sales,
	dim_store_details.store_type, 
	dim_store_details.country_code
FROM orders_table
LEFT JOIN dim_products ON orders_table.product_code=dim_products.product_code 
LEFT JOIN dim_store_details ON orders_table.store_code=dim_store_details.store_code
WHERE dim_store_details.country_code = 'DE'
GROUP BY store_type, country_code
ORDER BY total_sales;

/* TASK 9: Query the database to find average time between transaction for each year. */

WITH time_string AS 
(SELECT TO_TIMESTAMP(CONCAT("year", '-', "month", '-', "day", ' ', "timestamp"), 'YYYY-MM-DD HH24:MI:SS') AS combined_string
    FROM dim_date_times), time_lag AS
(SELECT EXTRACT(year FROM combined_string) AS year,
        EXTRACT( epoch FROM combined_string - LAG(combined_string) OVER (ORDER BY combined_string)) AS time_difference
    FROM time_string)
SELECT year, TO_CHAR(INTERVAL '1 second' * AVG(time_difference),
        '"hours": HH, "minutes": MI, "seconds": SS, "milliseconds": MS') AS actual_time_taken 
FROM time_lag
GROUP BY year
ORDER BY actual_time_taken DESC;
