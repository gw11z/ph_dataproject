/******** 1 ********/
CREATE OR REPLACE VIEW "USER_GW11Z"."PUBLIC".TOTAL_NUMBER_OF_FLIGHTS_V AS
SELECT f.year, 
       CASE f.month WHEN 1 THEN 'January'
                    WHEN 2 THEN 'February'
                    WHEN 3 THEN 'March'
                    WHEN 4 THEN 'April'
                    WHEN 5 THEN 'May'
                    WHEN 6 THEN 'June'
                    WHEN 7 THEN 'July'
                    WHEN 8 THEN 'August'
                    WHEN 9 THEN 'September'
                    WHEN 10 THEN 'October'
                    WHEN 11 THEN 'November'
                    WHEN 12 THEN 'December'
       END AS month,
       a.airline,
       i.airport,
       COUNT(1) AS number_of_flights
FROM "USER_GW11Z"."PUBLIC"."FLIGHTS" f
JOIN "USER_GW11Z"."PUBLIC"."AIRLINES" a ON f.airline = a.iata_code
JOIN "USER_GW11Z"."PUBLIC"."AIRPORTS" i ON f.origin_airport = i.iata_code
GROUP BY f.year,
         f.month,
         a.airline,
         i.airport
ORDER BY f.year,
         f.month,
         a.airline,
         i.airport;

/******** 2 ********/
CREATE OR REPLACE VIEW "USER_GW11Z"."PUBLIC".ONTIME_PCT_AIRLINE_V AS
WITH total_flights AS (
SELECT a.year,
       b.iata_code,
       b.airline,
       COUNT(1) AS tot_num_flights
  FROM "USER_GW11Z"."PUBLIC"."FLIGHTS" a
  JOIN "USER_GW11Z"."PUBLIC"."AIRLINES" b ON a.airline = b.iata_code
GROUP BY a.year,
         b.iata_code,
         b.airline
),
ontime_flights AS (
SELECT f.year,
       c.iata_code,
       c.airline,
       COUNT(1) AS ontime_num_flights
 FROM "USER_GW11Z"."PUBLIC"."FLIGHTS" f
  JOIN "USER_GW11Z"."PUBLIC"."AIRLINES" c ON f.airline = c.iata_code
WHERE f.arrival_delay <= 0
GROUP BY f.year, 
         c.iata_code,
         c.airline
)
SELECT t.year,
       t.airline,
       CAST(NVL(ontime_num_flights, 0)/t.tot_num_flights AS DECIMAL(3,2)) AS ontime_percentage
FROM total_flights t
LEFT JOIN ontime_flights o
ON t.year = o.year AND t.iata_code = o.iata_code
ORDER BY t.year,
         t.airline;

/******** 3 ********/
CREATE OR REPLACE VIEW "USER_GW11Z"."PUBLIC".AIRLINE_DELAYS_V AS
SELECT a.airline,
       COUNT(1) AS NUM_DELAYS
FROM "USER_GW11Z"."PUBLIC"."FLIGHTS" f
JOIN "USER_GW11Z"."PUBLIC"."AIRLINES" a ON f.airline = a.iata_code
WHERE NVL(f.departure_delay, 0) > 0
OR NVL(f.arrival_delay, 0) > 0
OR NVL(f.air_system_delay, 0) > 0
OR NVL(f.security_delay, 0) > 0
OR NVL(f.airline_delay, 0) > 0
OR NVL(f.late_aircraft_delay, 0) > 0
OR NVL(f.weather_delay, 0) > 0
GROUP BY a.airline
ORDER BY 2 DESC;

/******** 4 ********/
CREATE OR REPLACE VIEW "USER_GW11Z"."PUBLIC".AIRPORT_CANCEL_REASON_V AS
SELECT a.airport,
       CASE f.cancellation_reason WHEN 'A' THEN 'Airline/Carrier'
                                  WHEN 'B' THEN 'Weather'
                                  WHEN 'C' THEN 'National Air System'
                                  WHEN 'D' THEN 'Security'
                                  ELSE 'Other'
       END AS cancellation_reason,
       COUNT(1) AS cancellation_count
FROM "USER_GW11Z"."PUBLIC"."FLIGHTS" f
JOIN "USER_GW11Z"."PUBLIC"."AIRPORTS" a ON f.origin_airport = a.iata_code
WHERE f.cancellation_reason IS NOT NULL
GROUP BY a.airport,
         f.cancellation_reason
ORDER BY a.airport,
         f.cancellation_reason;

/******** 5 ********/
CREATE OR REPLACE VIEW "USER_GW11Z"."PUBLIC".AIRPORT_DELAY_REASON_V AS
SELECT a.airport,
       SUM(CASE WHEN f.departure_delay > 0 THEN 1 ELSE 0 END) AS departure_delay,
       SUM(CASE WHEN f.arrival_delay > 0 THEN 1 ELSE 0 END) AS arrival_delay,
       SUM(CASE WHEN f.air_system_delay > 0 THEN 1 ELSE 0 END) air_system_delay,
       SUM(CASE WHEN f.security_delay > 0 THEN 1 ELSE 0 END) AS security_delay,
       SUM(CASE WHEN f.airline_delay > 0 THEN 1 ELSE 0 END) AS airline_delay,
       SUM(CASE WHEN f.late_aircraft_delay > 0 THEN 1 ELSE 0 END) AS late_aircraft_delay,
       SUM(CASE WHEN f.weather_delay > 0 THEN 1 ELSE 0 END) AS weather_delay
FROM "USER_GW11Z"."PUBLIC"."FLIGHTS" f
JOIN "USER_GW11Z"."PUBLIC"."AIRPORTS" a ON f.origin_airport = a.iata_code
GROUP BY a.airport
ORDER BY 1;

/******** 6 ********/
CREATE OR REPLACE VIEW "USER_GW11Z"."PUBLIC".AIRLINE_UNIQUE_ROUTE_V AS
SELECT a.airline,
       COUNT(DISTINCT f.origin_airport, f.destination_airport) + COUNT(DISTINCT f.destination_airport, f.origin_airport) AS distinct_route_count
FROM "USER_GW11Z"."PUBLIC"."FLIGHTS" f JOIN "USER_GW11Z"."PUBLIC"."AIRLINES" a ON f.airline = a.iata_code
GROUP BY a.airline
ORDER BY 2 DESC;

/******** CLEAN UP ********/
DROP VIEW IF EXISTS "USER_GW11Z"."PUBLIC".TOTAL_NUMBER_OF_FLIGHTS_V;
DROP VIEW IF EXISTS "USER_GW11Z"."PUBLIC".ONTIME_PCT_AIRLINE_V;
DROP VIEW IF EXISTS "USER_GW11Z"."PUBLIC".AIRLINE_DELAYS_V;
DROP VIEW IF EXISTS "USER_GW11Z"."PUBLIC".AIRPORT_CANCEL_REASON_V;
DROP VIEW IF EXISTS "USER_GW11Z"."PUBLIC".AIRPORT_DELAY_REASON_V;
DROP VIEW IF EXISTS "USER_GW11Z"."PUBLIC".AIRLINE_UNIQUE_ROUTE_V;