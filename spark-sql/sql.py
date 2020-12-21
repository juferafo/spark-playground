# -*- coding: utf-8 -*-
"""
This script is intended to be used to test Spark SQL functionalities
"""

# importing spark and other libraries
from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("SparkExampleSQL")\
    .getOrCreate()

csv_path = "./flights/departuredelays.csv"

df = (spark.read.csv(path = csv_path, inferSchema = True, header = True))

# we create a temporary view so SQL queries can be issued against it
df.createOrReplaceTempView("us_delay_flights_tbl")

df.printSchema()

# First, we’ll find all flights whose distance is greater than 1,000 miles
query = """
    SELECT * 
    FROM us_delay_flights_tbl 
    WHERE distance > 1000
    ORDER BY distance DESC
"""
spark.sql(query)
#.show(10)

# Next, we’ll find all flights between San Francisco (SFO) and Chicago
# (ORD) with at least a two-hour delay
query = """
    SELECT * 
    FROM us_delay_flights_tbl
    WHERE (origin = "SFO" AND destination = "ORD") 
    AND (delay > 120)
    ORDER BY delay DESC
"""
spark.sql(query)
#.show(10)

# convert the date column into a readable format and find
# the days or months when these delays were most common.
query = """
    WITH temp AS (
        SELECT CAST(date as timestamp), delay 
        FROM us_delay_flights_tbl
        WHERE (origin = "SFO" AND destination = "ORD") 
        AND (delay > 120)
        ORDER BY delay DESC
    )
    SELECT 
        EXTRACT(MONTH FROM date) AS month,
        EXTRACT(DAY FROM date) AS day,
        COUNT(delay) AS count_delay
    FROM temp
    GROUP BY month, day
    ORDER BY count_delay DESC
"""
spark.sql(query)
#.show(10)

# Let's test a CASE clause in SQL
query = """
SELECT 
    delay, origin, destination,
CASE
    WHEN delay > 360 THEN 'Very Long Delays'
    WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
    WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
    WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
    WHEN delay = 0 THEN 'No Delays'
    ELSE 'Early'
END AS Flight_Delays
FROM us_delay_flights_tbl
ORDER BY origin, delay DESC
"""
spark.sql(query)#.show(10)

# same queries can be run with DataFrame API 
from pyspark.sql.functions import *

df.select("distance", "origin", "destination")\
    .where(col("distance") > 1000)\
    .orderBy(desc("distance"))#.show(10)

# Next, we’ll find all flights between San Francisco (SFO) and Chicago
# (ORD) with at least a two-hour delay

df.select("distance", "origin", "destination", "delay")\
    .where(col("delay") > 120)\
    .orderBy(col("delay"), ascending = False)#.show(10)