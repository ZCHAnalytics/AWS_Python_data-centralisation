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