CREATE TABLE daily_consumption_april2015 AS
SELECT 
    house_id,
    condate AS consumption_date,
    ROUND(MAX(energy_reading) - MIN(energy_reading), 2) AS daily_consumption_kwh
FROM consumption_single
WHERE condate LIKE '2015-04%'
GROUP BY house_id, condate
ORDER BY house_id, condate;
