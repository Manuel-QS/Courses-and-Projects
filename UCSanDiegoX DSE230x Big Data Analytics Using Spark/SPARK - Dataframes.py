# -*- coding: utf-8 -*-
"""
@author: ManuelQuintoSabelli
April 2023
UCSanDiegoX DSE230x
Big Data Analytics Using Spark
"""

"""
# SPARK - Exercises introduction
"""

#-----------------------------------------------------------------------------

"""
SPARK DATAFRAMES
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark import SparkContext
sc = SparkContext(master="local[4]")

sc.version
Out[57]: '3.5.1'



spark = SparkSession.builder \
         .master("local") \
         .appName("Word Count") \
         .config("spark.some.config.option", "some-value") \
         .getOrCreate()
         
         
df = spark.read.parquet('python/test_support/sql/parquet_partitioned')         
#-----------------------------------------------------------------------------

weather_parquet = './Data/NY.parquet'
df = sqlContext.read.load(weather_parquet)
df.show(1)


+-----------+-----------+----+--------------------+-----------------+--------------+------------------+-----------------+-----+-----------------+
|    Station|Measurement|Year|              Values|       dist_coast|      latitude|         longitude|        elevation|state|             name|
+-----------+-----------+----+--------------------+-----------------+--------------+------------------+-----------------+-----+-----------------+
|USW00094704|   PRCP_s20|1945|[00 00 00 00 00 0...|361.8320007324219|42.57080078125|-77.71330261230469|208.8000030517578|   NY|DANSVILLE MUNI AP|
+-----------+-----------+----+--------------------+-----------------+--------------+------------------+-----------------+-----+-----------------+
only showing top 1 row

#-----------------------------------------------------------------------------

#selecting a subset of the rows so it fits in slide.
df.select('station','year','measurement').show(5)

+-----------+----+-----------+
|    station|year|measurement|
+-----------+----+-----------+
|USW00094704|1945|   PRCP_s20|
|USW00094704|1946|   PRCP_s20|
|USW00094704|1947|   PRCP_s20|
|USW00094704|1948|   PRCP_s20|
|USW00094704|1949|   PRCP_s20|
+-----------+----+-----------+
only showing top 5 rows

#-----------------------------------------------------------------------------

"""
Dataframes are an efficient way to store data tables.
All of the values in a column have the same type.
A good way to store a dataframe in disk is to use a Parquet file.
"""