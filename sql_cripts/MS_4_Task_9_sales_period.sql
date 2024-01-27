WITH time_string AS 
(
    SELECT 
        TO_TIMESTAMP(CONCAT("year", '-', "month", '-', "day", ' ', "timestamp"), 'YYYY-MM-DD HH24:MI:SS') AS combined_string
    FROM dim_date_times
), time_lag AS
(
    SELECT 
        EXTRACT(year FROM combined_string) AS year,
        EXTRACT( epoch FROM combined_string - LAG(combined_string) OVER (ORDER BY combined_string)) AS time_difference
    FROM time_string
)
SELECT
    year,
    TO_CHAR(
        INTERVAL '1 second' * AVG(time_difference),
        '"hours": HH, "minutes": MI, "seconds": SS, "milliseconds": MS'
    ) AS actual_time_taken 
FROM 
    time_lag
GROUP BY year
ORDER BY actual_time_taken DESC;
