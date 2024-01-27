/* Task 1: Cast columns into the correct datatype. */

DO $$ 
DECLARE 
    max_card_number_length INT;
    max_store_code_length INT;
    max_product_code_length INT;
BEGIN
    SELECT 
        MAX(LENGTH(CAST(card_number AS TEXT))),
        MAX(LENGTH(store_code)),
        MAX(LENGTH(product_code))
    INTO 
        max_card_number_length,
        max_store_code_length,
        max_product_code_length
    FROM 
        orders_table;
    
    EXECUTE 'ALTER TABLE orders_table
             ALTER COLUMN card_number TYPE VARCHAR(' || max_card_number_length || ')';
    EXECUTE 'ALTER TABLE orders_table
             ALTER COLUMN store_code TYPE VARCHAR(' || max_store_code_length || ')';
    EXECUTE 'ALTER TABLE orders_table
             ALTER COLUMN product_code TYPE VARCHAR(' || max_product_code_length || ')';
	EXECUTE 'ALTER TABLE orders_table
			 Alter COLUMN user_uuid TYPE UUID USING user_uuid::UUID';
	EXECUTE 'ALTER TABLE orders_table
			ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID';
	EXECUTE 'ALTER TABLE orders_table
			ALTER COLUMN product_quantity TYPE SMALLINT';
END $$;
SELECT * FROM orders_table;


/* Task 2: Cast columns into the correct datatype. */
DO $$ 
DECLARE 
    max_country_code_length INT;
BEGIN
    SELECT 
        MAX(LENGTH(country_code))
    INTO 
        max_country_code_length
    FROM 
        dim_users;
    
    EXECUTE 'ALTER TABLE dim_users
             ALTER COLUMN first_name TYPE VARCHAR(255)';
    EXECUTE 'ALTER TABLE dim_users
             ALTER COLUMN last_name TYPE VARCHAR(255)';
    EXECUTE 'ALTER TABLE dim_users
             ALTER COLUMN date_of_birth TYPE DATE';
    EXECUTE 'ALTER TABLE dim_users
             ALTER COLUMN country_code TYPE VARCHAR(' || max_country_code_length || ')';
    EXECUTE 'ALTER TABLE dim_users
             ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID';
    EXECUTE 'ALTER TABLE dim_users
			ALTER COLUMN join_date TYPE DATE';
END $$;
SELECT * FROM dim_users;

/* 





store datatype goes here



*/

/* Task 4 and 5: Update and Cast columns into the correct datatype. */

ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

UPDATE dim_products
SET still_available = TRIM(LOWER(still_available));

-- update still_available column
UPDATE dim_products
SET still_available =
	CASE
		WHEN still_available = 'still_avaliable' THEN 'yes'
		WHEN still_available = 'removed' THEN 'no'
		ELSE NULL
	END;
-- remove currency sign from product_price
UPDATE dim_products
SET product_price = CAST(REPLACE(CAST(product_price AS VARCHAR), '£', '') AS FLOAT);

-- create weight_class column
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(255);

UPDATE dim_products
SET weight_class = 
	CASE
		WHEN weight < 2 THEN 'Light'
		WHEN weight >= 2 AND weight < 40 THEN 'Mid-Sized'
		WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
		ELSE 'Truck_Required'
	END;

DO $$
DECLARE
	max_ean_length INT;
	max_product_code_length INT;
	max_weight_class_length INT;
BEGIN
	SELECT 
		MAX(LENGTH(LOWER("EAN"))),
		MAX(LENGTH(product_code)),
		MAX(LENGTH(weight_class))
	INTO 
		max_ean_length, 
		max_product_code_length, 
		max_weight_class_length
	FROM dim_products;
	
	EXECUTE 'ALTER TABLE dim_products
			ALTER COLUMN weight TYPE FLOAT';
	EXECUTE 'ALTER TABLE dim_products
			ALTER COLUMN "EAN" TYPE VARCHAR(' || max_ean_length || ')';
	EXECUTE 'ALTER TABLE dim_products
			ALTER COLUMN product_code TYPE VARCHAR(' || max_product_code_length || ')';
	EXECUTE 'ALTER TABLE dim_products
			ALTER COLUMN date_added TYPE DATE';
	EXECUTE 'ALTER TABLE dim_products
			ALTER COLUMN uuid TYPE UUID USING uuid::UUID';
	EXECUTE 'ALTER TABLE dim_products
			ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT';
	EXECUTE 'ALTER TABLE dim_products
			ALTER COLUMN still_available TYPE BOOL USING still_available::BOOL';
	EXECUTE 'ALTER TABLE dim_products
			ALTER COLUMN weight_class TYPE VARCHAR(' || max_weight_class_length || ')';
END $$;
SELECT DISTINCT still_available, COUNT(*) AS total_count FROM dim_products
GROUP BY still_available;

SELECT * FROM dim_products;

/* Task 6: Cast the columns into the correct format. */

DO $$
DECLARE 
	max_time_period_length INT;
BEGIN 
	SELECT MAX(LENGTH(time_period)) INTO max_time_period_length FROM dim_date_times;
	EXECUTE 'ALTER TABLE dim_date_times
	ALTER COLUMN "month" TYPE VARCHAR(2)';
	EXECUTE 'ALTER TABLE dim_date_times
	ALTER COLUMN "year" TYPE VARCHAR(4)';
	EXECUTE 'ALTER TABLE dim_date_times
	ALTER COLUMN "day" TYPE VARCHAR(2)';
	EXECUTE 'ALTER TABLE dim_date_times
	ALTER COLUMN time_period TYPE VARCHAR(' || max_time_period_length || ')';
	EXECUTE 'ALTER TABLE dim_date_times
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID';
END $$;

SELECT * FROM dim_date_times;


/* Task 7: Cast columns into the correct datatype. */

DO $$
DECLARE
	max_card_number_length INT;
	max_expiry_date_length INT;
BEGIN
	SELECT 
		MAX(LENGTH(card_number)),
		MAX(LENGTH(expiry_date))
	INTO 
		max_card_number_length,
		max_expiry_date_length
	FROM dim_card_details;
	
	EXECUTE 'ALTER TABLE dim_card_details
			ALTER COLUMN card_number TYPE VARCHAR(' || max_card_number_length || ')';
	EXECUTE 'ALTER TABLE dim_card_details
			ALTER COLUMN expiry_date TYPE VARCHAR(' || max_expiry_date_length || ')';
	EXECUTE 'ALTER TABLE dim_card_details
			ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE';
END $$;
SELECT * FROM dim_card_details;


/* Task 8: Add primary keys. */

ALTER TABLE dim_date_times 
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

/* Task 9: Add foreing keys. */

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_user
FOREIGN KEY (user_uuid)
REFERENCES dim_users(user_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_card
FOREIGN KEY (card_number)
REFERENCES dim_card_details(card_number);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_date
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times(date_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_store
FOREIGN KEY (store_code)
REFERENCES dim_store_details(store_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_products
FOREIGN KEY (product_code)
REFERENCES dim_products(product_code);

-- MILESTONE 3

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

-- END OF SCRIPT
