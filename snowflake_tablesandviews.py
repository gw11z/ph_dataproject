import snowflake.connector

#create connection
conn=snowflake.connector.connect(
      user='gw11z',
      password='ETMsDgt86',
      account='zm31703.us-east-2.aws'
      )

#create cursor
#curs=conn.cursor()

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
    
#    sql = 'drop table if exists student_math_mark'
#    execute_query(conn, sql)

#    sql = 'create table student_math_mark(name varchar, mark double)'
#    execute_query(conn, sql)







#    sql = 'copy into student_math_mark from @DATA_STAGE/Student_marks.csv.gz file_format = (type = "csv" field_delimiter = "," skip_header = 1)' \
#          'ON_ERROR = "ABORT_STATEMENT" '
#    execute_query(conn, sql)


#close cursor
#finally:
#    curs.close()

#close connection
#conn.close()

except Exception as e:
    print(e)