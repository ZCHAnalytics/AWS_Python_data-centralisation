
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