#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
In this script I am testing a map-reduce workflow with RDDs
"""

from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local")
sc = SparkContext.getOrCreate()

dataRDD = sc.parallelize(
    [("Brooke",20),\
     ("Denni",31),\
     ("Jules",30),\
     ("TD",35),\
     ("Brooke",25)
     ])

# Group by Name
agesRDD = dataRDD \
    .map(lambda x: (x[0], (x[1],1))) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .map(lambda x: (x[0], x[1][1]))
        
print(agesRDD.collect())

# Compute the average of the ages 
ages_avv = dataRDD \
    .map(lambda x: (x[0], (x[1],1))) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .map(lambda x: (x[0], x[1][0]/x[1][1]))
    
print(ages_avv.collect())

# In general RDDs and lambda functions are cryptic and hard to read
# It does not allow Spark to optimize the execution as we are telling
# to Spark how to carry out the calculation step by step

# Let's use SQL and Spark Dataframes

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

ss = SparkSession.builder.getOrCreate()

data_df = ss.createDataFrame(data = dataRDD, schema = ["name","age"])

data_average = data_df\
    .groupBy(data_df.name)\
    .agg(avg(data_df.age))

print(data_average.show())
