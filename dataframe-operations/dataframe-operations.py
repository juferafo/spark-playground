#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script contains excercices of dataframe operations

data from repo -> https://github.com/databricks/LearningSparkV2.git
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *

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
#data_df.show(10)

# like spark.read spark.write can be use in the same way

# Basic operations with dataframes in Spark

# Saving Dataframe as parquet or SQL table

#data_df.write.format("parquet").save("/Users/mbjuan/Desktop/spark-playground/dataframe-operations/data.parquet")

data_df.write.format("parquet").saveAsTable("table")

spark.stop()

