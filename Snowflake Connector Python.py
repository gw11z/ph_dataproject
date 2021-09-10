# Databricks notebook source
# snowflake connection options
options = {
  "sfUrl": "https://zm31703.us-east-2.aws.snowflakecomputing.com/",
  "sfUser": "gw11z",
  "sfPassword": "ETMsDgt86",
  "sfDatabase": "USER_GW11Z",
  "sfSchema": "PUBLIC",
  "sfWarehouse": "COMPUTE_WH"
}

# COMMAND ----------

# Read the Airlines Data
AirlinesDf = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "AIRLINES") \
  .load()

display(AirlinesDf)

# COMMAND ----------

# Read the Airlines Data
AirportsDf = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "AIRPORTS") \
  .load()

display(AirportsDf)

# COMMAND ----------

# Read the Flights Data
FlightsDf = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "FLIGHTS") \
  .load()

display(FlightsDf)

# COMMAND ----------

# Read the View 1 Data
Report1Df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "TOTAL_NUMBER_OF_FLIGHTS_V") \
  .load()

display(Report1Df)

# COMMAND ----------

# Read the View 2 Data
Report2Df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "ONTIME_PCT_AIRLINE_V") \
  .load()

display(Report2Df)

# COMMAND ----------

# Read the View 3 Data
Report3Df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "AIRLINE_DELAYS_V") \
  .load()

display(Report3Df)

# COMMAND ----------

# Read the View 4 Data
Report4Df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "AIRPORT_CANCEL_REASON_V") \
  .load()

display(Report4Df)

# COMMAND ----------

# Read the View 5 Data
Report5Df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "AIRPORT_DELAY_REASON_V") \
  .load()

display(Report5Df)

# COMMAND ----------

# Read the View 6 Data
Report6Df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "AIRLINE_UNIQUE_ROUTE_V") \
  .load()

display(Report6Df)

# COMMAND ----------


