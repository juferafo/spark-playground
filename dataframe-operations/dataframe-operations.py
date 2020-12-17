#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script contains excercices of dataframe operations

data from repo -> https://github.com/databricks/LearningSparkV2.git
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .getOrCreate()

# path to data 
# this can be improved by making use of sys.argv[1]
data_path = "/Users/mbjuan/Desktop/spark-playground/dataframe-operations/data/sf-fire/sf-fire-calls.csv"

# schema definition
fire_schema = StructType([
    StructField('CallNumber', IntegerType(), True),
    StructField('UnitID', StringType(), True),
    StructField('IncidentNumber', IntegerType(), True),
    StructField('CallType', StringType(), True),
    StructField('CallDate', StringType(), True),
    StructField('WatchDate', StringType(), True),
    StructField('CallFinalDisposition', StringType(), True),
    StructField('AvailableDtTm', StringType(), True),
    StructField('Address', StringType(), True),
    StructField('City', StringType(), True),
    StructField('Zipcode', IntegerType(), True),
    StructField('Battalion', StringType(), True),
    StructField('StationArea', StringType(), True),
    StructField('Box', StringType(), True),
    StructField('OriginalPriority', StringType(), True),
    StructField('Priority', StringType(), True),
    StructField('FinalPriority', IntegerType(), True),
    StructField('ALSUnit', BooleanType(), True),
    StructField('CallTypeGroup', StringType(), True),
    StructField('NumAlarms', IntegerType(), True),
    StructField('UnitType', StringType(), True),
    StructField('UnitSequenceInCallDispatch', IntegerType(), True),
    StructField('FirePreventionDistrict', StringType(), True),
    StructField('SupervisorDistrict', StringType(), True),
    StructField('Neighborhood', StringType(), True),
    StructField('Location', StringType(), True),
    StructField('RowID', StringType(), True),
    StructField('Delay', FloatType(), True)
    ])

data_df = spark.read.csv(data_path, schema=fire_schema, header=True)

# like spark.read spark.write can be use in the same way to save the Dataframe
#   1. as a parquet file
#   2. as an SQL table

#data_df.write.format("parquet").save("/Users/mbjuan/Desktop/spark-playground/dataframe-operations/data.parquet")
#data_df.write.format("parquet").saveAsTable("table")

# we can perform operations (transformations or actions) over the data

new_fire_df = data_df\
    .select("IncidentNumber", "AvailableDtTm", "CallType")\
    .where(data_df.CallType == "Alarms")
    
#new_fire_df.show(truncate = False)
    
# How many distinct CallTypes were recorded as the causes of the fire calls?

distinct_CallTypes = data_df\
    .select("CallType")\
    .where(col("CallType").isNotNull())\
    .agg(countDistinct("CallType"))
    
distinct_CallTypes = data_df\
    .select("CallType")\
    .where(col("CallType").isNotNull())\
    .distinct()
    
# Renaming columns

rename_delay = data_df.withColumnRenamed("Delay", "ResponseDelayedinMins")\

rename_delay = rename_delay\
    .select("ResponseDelayedinMins")\
    .filter(col("ResponseDelayedinMins") > 5)
    
# Convert fields into timestamp or date

fire_ts_df = data_df\
    .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))\
    .drop("CallDate")\
    .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))\
    .drop("WatchDate")\
    .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),"MM/dd/yyyy hh:mm:ss a"))\
    .drop("AvailableDtTm")\
    .select("IncidentDate", "AvailableDtTS", "OnWatchDate")

fire_ts_df.printSchema()

#fire_ts_df.show(truncate = False)

# how many calls were logged in the last seven days?

last_week_df = fire_ts_df\
    .withColumn("week", weekofyear(col("IncidentDate")))\
    .withColumn("year", year(col("IncidentDate")))\
    .groupBy("year", "week").agg(count("IncidentDate"))\
    .orderBy("year", "week", ascending=False)\
    .show()
    




spark.stop()

