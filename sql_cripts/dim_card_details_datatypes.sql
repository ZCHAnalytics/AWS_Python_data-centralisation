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