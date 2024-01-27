

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
