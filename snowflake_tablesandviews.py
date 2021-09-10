import snowflake.connector

#create connection
conn=snowflake.connector.connect(
      user='gw11z',
      password='ETMsDgt86',
      account='zm31703.us-east-2.aws'
      )

def execute_query(connection, query):
    print(query)
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()

try:
#    sql = 'use role {}'.format('SYSADMIN')
#    execute_query(conn, sql)

    sql = 'use database {}'.format('USER_GW11Z')
    execute_query(conn, sql)

    sql = 'use warehouse {}'.format('INTERVIEW_WH')
    execute_query(conn, sql)

    sql = 'use schema {}'.format('PUBLIC')
    execute_query(conn, sql)

    sql = 'create or replace table airlines(iata_code varchar, airline varchar)'
    execute_query(conn, sql)

    sql = 'create or replace table airports(iata_code varchar, airport varchar, city varchar, state varchar, country varchar, latitude number, logitude number)'
    execute_query(conn, sql)

    sql = 'create or replace table flights(year number, month number, day number, day_of_week number,  airline string, flight_number string, tail_number string, origin_airport string, destination_airport string, scheduled_departure string, departure_time string, departure_delay number, taxi_out number, wheels_off string, scheduled_time number, elapsed_time number, air_time number, distance number, wheels_on number, taxi_in number, scheduled_arrival number, arrival_time string, arrival_delay string, diverted number, cancelled number, cancellation_reason string, air_system_delay number, security_delay number, airline_delay number, late_aircraft_delay number, weather_delay number)'
    execute_query(conn, sql)

    sql = 'drop stage if exists airlines_stage'
    execute_query(conn, sql)

    sql = 'create stage airlines_stage file_format = (type = "csv" field_delimiter = "," skip_header = 1)'
    execute_query(conn, sql)

    sql = 'drop stage if exists airports_stage'
    execute_query(conn, sql) 

    sql = 'create stage airports_stage file_format = (type = "csv" field_delimiter = "," skip_header = 1)'
    execute_query(conn, sql)
 
    sql = 'drop stage if exists flights_stage'
    execute_query(conn, sql)
    
    sql = 'create stage flights_stage file_format = (type = "csv" field_delimiter = "," skip_header = 1)'
    execute_query(conn, sql)
    
    csv_file = 'C:\\Users\\Geoff\\Downloads\\airlines.csv'
    sql = "PUT file://" + csv_file + " @AIRLINES_STAGE auto_compress=true"
    execute_query(conn, sql)

    sql = 'copy into AIRLINES from @AIRLINES_STAGE file_format = (type = "csv" field_delimiter = "," skip_header = 1)' \
          'ON_ERROR = "ABORT_STATEMENT" '
    execute_query(conn, sql)
    
    csv_file = 'C:\\Users\\Geoff\\Downloads\\airports.csv'
    sql = "PUT file://" + csv_file + " @AIRPORTS_STAGE auto_compress=true"
    execute_query(conn, sql)

    sql = 'copy into AIRPORTS from @AIRPORTS_STAGE file_format = (type = "csv" field_delimiter = "," skip_header = 1)' \
          'ON_ERROR = "ABORT_STATEMENT" '
    execute_query(conn, sql)

    csv_file = 'C:\\Users\\Geoff\\Downloads\\partition-*.csv'
    sql = "PUT file://" + csv_file + " @FLIGHTS_STAGE auto_compress=true"
    execute_query(conn, sql)

    sql = 'copy into FLIGHTS from @FLIGHTS_STAGE file_format = (type = "csv" field_delimiter = "," skip_header = 1)' \
          ' pattern = ".*partition-0[1-8].csv.gz" ON_ERROR = "ABORT_STATEMENT" '
    execute_query(conn, sql)

    sql = """CREATE OR REPLACE VIEW "USER_GW11Z"."PUBLIC".TOTAL_NUMBER_OF_FLIGHTS_V AS 
          SELECT f.year, CASE f.month WHEN 1 THEN \'January\' WHEN 2 THEN \'February\' WHEN 3 THEN \'March\' WHEN 4 THEN \'April\' WHEN 5 THEN \'May\' WHEN 6 THEN \'June\' WHEN 7 THEN \'July\' WHEN 8 THEN \'August\' WHEN 9 THEN \'September\' WHEN 10 THEN \'October\' WHEN 11 THEN \'November\' WHEN 12 THEN \'December\' END AS month, a.airline, i.airport, COUNT(1) AS number_of_flights 
          FROM "USER_GW11Z"."PUBLIC"."FLIGHTS" f 
          JOIN "USER_GW11Z"."PUBLIC"."AIRLINES" a ON f.airline = a.iata_code 
          JOIN "USER_GW11Z"."PUBLIC"."AIRPORTS" i ON f.origin_airport = i.iata_code 
          GROUP BY f.year, f.month, a.airline, i.airport 
          ORDER BY f.year, f.month, a.airline, i.airport """
    execute_query(conn, sql)

    sql = """CREATE OR REPLACE VIEW "USER_GW11Z"."PUBLIC".ONTIME_PCT_AIRLINE_V AS
          WITH total_flights AS (
          SELECT a.year, b.iata_code, b.airline, COUNT(1) AS tot_num_flights
          FROM "USER_GW11Z"."PUBLIC"."FLIGHTS" a
          JOIN "USER_GW11Z"."PUBLIC"."AIRLINES" b ON a.airline = b.iata_code
          GROUP BY a.year, b.iata_code, b.airline ),
          ontime_flights AS (
          SELECT f.year, c.iata_code, c.airline, COUNT(1) AS ontime_num_flights
          FROM "USER_GW11Z"."PUBLIC"."FLIGHTS" f
          JOIN "USER_GW11Z"."PUBLIC"."AIRLINES" c ON f.airline = c.iata_code
          WHERE f.arrival_delay <= 0
          GROUP BY f.year, c.iata_code,c.airline )
          SELECT t.year, t.airline, CAST(NVL(ontime_num_flights, 0)/t.tot_num_flights AS DECIMAL(3,2)) AS ontime_percentage
          FROM total_flights t
          LEFT JOIN ontime_flights o
          ON t.year = o.year AND t.iata_code = o.iata_code
          ORDER BY t.year, t.airline """
    execute_query(conn, sql)

    sql = """CREATE OR REPLACE VIEW "USER_GW11Z"."PUBLIC".AIRLINE_DELAYS_V AS
          SELECT a.airline, COUNT(1) AS NUM_DELAYS
          FROM "USER_GW11Z"."PUBLIC"."FLIGHTS" f
          JOIN "USER_GW11Z"."PUBLIC"."AIRLINES" a ON f.airline = a.iata_code
          WHERE NVL(f.departure_delay, 0) > 0 OR NVL(f.arrival_delay, 0) > 0 OR NVL(f.air_system_delay, 0) > 0 OR NVL(f.security_delay, 0) > 0 OR NVL(f.airline_delay, 0) > 0 OR NVL(f.late_aircraft_delay, 0) > 0 OR NVL(f.weather_delay, 0) > 0
          GROUP BY a.airline
          ORDER BY 2 DESC"""
    execute_query(conn, sql)

    sql = """CREATE OR REPLACE VIEW "USER_GW11Z"."PUBLIC".AIRPORT_CANCEL_REASON_V AS
          SELECT a.airport, CASE f.cancellation_reason WHEN \'A\' THEN \'Airline/Carrier\' WHEN \'B\' THEN 'Weather' WHEN \'C\' THEN 'National Air System' WHEN \'D\' THEN 'Security' ELSE \'Other\' END AS cancellation_reason, COUNT(1) AS cancellation_count
          FROM "USER_GW11Z"."PUBLIC"."FLIGHTS" f
          JOIN "USER_GW11Z"."PUBLIC"."AIRPORTS" a ON f.origin_airport = a.iata_code
          WHERE f.cancellation_reason IS NOT NULL
          GROUP BY a.airport, f.cancellation_reason
          ORDER BY a.airport, f.cancellation_reason"""
    execute_query(conn, sql)

    sql = """CREATE OR REPLACE VIEW "USER_GW11Z"."PUBLIC".AIRPORT_DELAY_REASON_V AS
          SELECT a.airport, SUM(CASE WHEN f.departure_delay > 0 THEN 1 ELSE 0 END) AS departure_delay, SUM(CASE WHEN f.arrival_delay > 0 THEN 1 ELSE 0 END) AS arrival_delay, SUM(CASE WHEN f.air_system_delay > 0 THEN 1 ELSE 0 END) air_system_delay, SUM(CASE WHEN f.security_delay > 0 THEN 1 ELSE 0 END) AS security_delay, SUM(CASE WHEN f.airline_delay > 0 THEN 1 ELSE 0 END) AS airline_delay, SUM(CASE WHEN f.late_aircraft_delay > 0 THEN 1 ELSE 0 END) AS late_aircraft_delay, SUM(CASE WHEN f.weather_delay > 0 THEN 1 ELSE 0 END) AS weather_delay
          FROM "USER_GW11Z"."PUBLIC"."FLIGHTS" f
          JOIN "USER_GW11Z"."PUBLIC"."AIRPORTS" a ON f.origin_airport = a.iata_code
          GROUP BY a.airport
          ORDER BY 1"""
    execute_query(conn, sql)

    sql = """CREATE OR REPLACE VIEW "USER_GW11Z"."PUBLIC".AIRLINE_UNIQUE_ROUTE_V AS
          SELECT a.airline, COUNT(DISTINCT f.origin_airport, f.destination_airport) + COUNT(DISTINCT f.destination_airport, f.origin_airport) AS distinct_route_count
          FROM "USER_GW11Z"."PUBLIC"."FLIGHTS" f JOIN "USER_GW11Z"."PUBLIC"."AIRLINES" a ON f.airline = a.iata_code
          GROUP BY a.airline
          ORDER BY 2 DESC"""
    execute_query(conn, sql)

except Exception as e:
    print(e)