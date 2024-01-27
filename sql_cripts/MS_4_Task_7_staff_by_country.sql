/* Task 6: Query database to find the overall staff numbers in each location around the world.  */

SELECT SUM(staff_numbers) AS total_staff_number, country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_number DESC;