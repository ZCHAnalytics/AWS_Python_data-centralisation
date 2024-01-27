SELECT COUNT(DISTINCT(product_code)) FROM orders_table;
SELECT * from dim_products;
SELECT COUNT(DISTINCT(product_code)) FROM dim_products;
SELECT COUNT(DISTINCT(uuid)) FROM dim_products; 

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