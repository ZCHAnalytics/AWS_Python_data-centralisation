/* TASK 2: Query the database to find which locations have the most stores currently */

SELECT locality, COUNT(store_code) AS stores_total from dim_store_details
GROUP BY locality
ORDER BY stores_total DESC, locality ASC LIMIT 7;