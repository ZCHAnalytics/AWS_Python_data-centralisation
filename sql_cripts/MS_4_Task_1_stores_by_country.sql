/* TASK 1: Query the database to find which countries we currently operate in and which now has the most stores. */

SELECT country_code, COUNT(store_code) AS total_no_stores FROM dim_store_details
GROUP by country_code 
ORDER BY total_no_stores DESC;
