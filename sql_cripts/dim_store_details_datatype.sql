--ALTER TABLE dim_store_details
--DROP COLUMN lat;
--SELECT * from dim_store_details;

UPDATE dim_store_details
SET longitude = NULL
WHERE longitude = 'N/A';

UPDATE dim_store_details
SET address = NULL
WHERE address = 'N/A';

UPDATE dim_store_details
SET latitude = NULL
WHERE latitude = 'N/A';

UPDATE dim_store_details
SET locality = NULL
WHERE locality = 'N/A';



DO $$
DECLARE 
    max_store_code_length INT;
BEGIN
    SELECT 
        MAX(LENGTH(store_code))
    INTO 
        max_store_code_length
    FROM 
        dim_store_details;
		
    EXECUTE 'ALTER TABLE dim_store_details
             ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT';
    EXECUTE 'ALTER TABLE dim_store_details
             ALTER COLUMN locality TYPE VARCHAR(255)';
    EXECUTE 'ALTER TABLE dim_store_details
             ALTER COLUMN store_code TYPE VARCHAR(' || max_store_code_length || ')';
    EXECUTE 'ALTER TABLE dim_store_details
             ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT';
    EXECUTE 'ALTER TABLE dim_store_details
             ALTER COLUMN opening_date TYPE DATE';
    EXECUTE 'ALTER TABLE dim_store_details
             ALTER COLUMN store_type TYPE VARCHAR(255)';
	EXECUTE 'ALTER TABLE dim_store_details
             ALTER COLUMN store_type DROP NOT NULL';
    EXECUTE 'ALTER TABLE dim_store_details
             ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT';
	EXECUTE 'ALTER TABLE dim_store_details
             ALTER COLUMN country_code TYPE VARCHAR(2)';
    EXECUTE 'ALTER TABLE dim_store_details
			ALTER COLUMN continent TYPE VARCHAR(255)';
END $$;
SELECT * FROM dim_store_details;

SELECT DISTINCT store_type from dim_store_details;

SELECT * from dim_store_details
WHERE store_type LIKE '%Web%';

