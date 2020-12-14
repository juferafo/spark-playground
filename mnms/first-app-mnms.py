# -*- coding: utf-8 -*-
"""
This script is used to count MNMs :)

usage:
    python first-app-mnm.py <data_file>
"""

import sys

# importing spark and other libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# if __name__ == "__main_":

"""if len(sys.argv) != 2:
    print("Usage: mnmcount <file>", file=sys.stderr)
    sys.exit(-1)"""

# Sparksession
# getOrCreate -> only one sparksession per JVM. If this one does not
# exist it will create one session (main entry point to use spark!)

spark = SparkSession \
    .builder \
    .appName("My first app") \
    .getOrCreate()

# get the MNM data. 
# This can be generated randomly with the script gen_mnm_dataset.py
try:
    mnm_file = sys.argv[1]
except:
    # For testing purposes. Modify this according to your needs
    mnm_file = "./data/mnm_dataset.csv"
    
print("Reading the file {}".format(mnm_file))

# reading the MNM data with spark
mnm_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(mnm_file)

"""
We are going to build a workflow with the following steps with spark:
    
    1. Select the fields State,Color,Count
    that is the equivalent of SELECT * in SQL for this case
    
    2. Group the data by each state and color
    
    3. Aggregate the color counts
"""

count_mnm_df = mnm_df \
    .select("State", "Color", "Count") \
    .groupBy("State", "Color") \
    .agg(count("Count").alias("Total")) \
    .orderBy("Total", ascending=True)

# So far only transformations were employed and, due to the lazy evaluation,
# no data was processed

count_mnm_df.show()
print("Total number of rows {}".format(count_mnm_df.count()))

# What about the results for a particular state like California CA?

count_mnm_CA_df = mnm_df \
    .select("State", "Color", "Count") \
    .filter(mnm_df.State == "CA") \
    .groupBy(mnm_df.State, mnm_df.Color) \
    .agg(count("Count").alias("Total")) \
    .orderBy("Total", ascending=False)

count_mnm_CA_df.show()

# Stop the SparkSession

spark.stop()
