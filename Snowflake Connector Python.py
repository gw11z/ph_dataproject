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


