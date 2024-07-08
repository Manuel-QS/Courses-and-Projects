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


import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

#Initialize and load weather dataframe

from pyspark import SparkContext
sc = SparkContext(master="local[4]")
#sc.version

import os
import sys

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType
%pylab inline

# Just like using Spark requires having a SparkContext, using SQL requires an SQLContext
sqlContext = SQLContext(sc)


#-----------------------------------------------------------------------------

data_dir=r"/resource/asnlib/publicdata/Data"

#-----------------------------------------------------------------------------

weather_parquet = data_dir+'/NY.parquet'
print(weather_parquet)
df = sqlContext.read.load(weather_parquet)
df.show(1)

+-----------+-----------+----+--------------------+-----------------+--------------+------------------+-----------------+-----+-----------------+
|    Station|Measurement|Year|              Values|       dist_coast|      latitude|         longitude|        elevation|state|             name|
+-----------+-----------+----+--------------------+-----------------+--------------+------------------+-----------------+-----+-----------------+
|USW00094704|   PRCP_s20|1945|[00 00 00 00 00 0...|361.8320007324219|42.57080078125|-77.71330261230469|208.8000030517578|   NY|DANSVILLE MUNI AP|
+-----------+-----------+----+--------------------+-----------------+--------------+------------------+-----------------+-----+-----------------+
only showing top 1 row

#-----------------------------------------------------------------------------


df.printSchema()

root
 |-- Station: string (nullable = true)
 |-- Measurement: string (nullable = true)
 |-- Year: long (nullable = true)
 |-- Values: binary (nullable = true)
 |-- dist_coast: double (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
 |-- elevation: double (nullable = true)
 |-- state: string (nullable = true)
 |-- name: string (nullable = true)
#-----------------------------------------------------------------------------

print(df.count())
df.show(1)

+-----------+-----------+----+--------------------+-----------------+--------------+------------------+-----------------+-----+-----------------+
|    Station|Measurement|Year|              Values|       dist_coast|      latitude|         longitude|        elevation|state|             name|
+-----------+-----------+----+--------------------+-----------------+--------------+------------------+-----------------+-----+-----------------+
|USW00094704|   PRCP_s20|1945|[00 00 00 00 00 0...|361.8320007324219|42.57080078125|-77.71330261230469|208.8000030517578|   NY|DANSVILLE MUNI AP|
+-----------+-----------+----+--------------------+-----------------+--------------+------------------+-----------------+-----+-----------------+
only showing top 1 row
#-----------------------------------------------------------------------------

"""
.describe()
The method df.describe() computes five statistics for each column of the dataframe df.

The statistics are: count, mean, std, min,max
"""

df.describe().select('station', 'elevation', 'measurement').show()

+-----------+------------------+-----------+
|    station|         elevation|measurement|
+-----------+------------------+-----------+
|     168398|            168398|     168398|
|       NULL| 245.2899639266881|       NULL|
|       NULL|189.69342701097085|       NULL|
|USC00300015|-999.9000244140625|       PRCP|
|USW00094794| 838.2000122070312|   TOBS_s20|
+-----------+------------------+-----------+
#-----------------------------------------------------------------------------

"""
groupby and agg
The method .groupby(col) groups rows according the value of the column col.
The method .agg(spec) computes a summary for each group as specified in spec
"""
df.groupby('measurement').agg({'year': 'min', 'station':'count'}).show()

+-----------+---------+--------------+
|measurement|min(year)|count(station)|
+-----------+---------+--------------+
|   TMIN_s20|     1873|         13442|
|       TMIN|     1873|         13442|
|   SNOW_s20|     1884|         15629|
|       TOBS|     1876|         10956|
|   SNWD_s20|     1888|         14617|
|   PRCP_s20|     1871|         16118|
|   TOBS_s20|     1876|         10956|
|       TMAX|     1873|         13437|
|       SNOW|     1884|         15629|
|   TMAX_s20|     1873|         13437|
|       SNWD|     1888|         14617|
|       PRCP|     1871|         16118|
+-----------+---------+--------------+
#-----------------------------------------------------------------------------

"""
Using SQL queries on DataFrames
There are two main ways to manipulate DataFrames:
    
Imperative manipulation
Using python methods such as .select and .groupby.

Advantage: order of operations is specified.
Disrdavantage : You need to describe both what is the result you want and how to get it.

Declarative Manipulation (SQL)
Advantage: You need to describe only what is the result you want.
Disadvantage: SQL does not have primitives for common analysis operations such as covariance

Using sql commands on a dataframe
Spark supports a subset of the Hive SQL query language.

For example, You can use Hive select syntax to select a subset of the rows in a dataframe.

To use sql on a dataframe you need to first register it as a TempTable.

for variety, we are using here a small dataframe loaded from a JSON file.
"""
#-----------------------------------------------------------------------------

# Counting the number of occurances of each measurement, imparatively

L=df.groupBy('measurement').count().collect()
#L is a list (collected DataFrame)

D=[(e.measurement,e['count']) for e in L]
sorted(D,key=lambda x:x[1], reverse=False)[:6]

Out[11]: 
[('TOBS', 10956),
 ('TOBS_s20', 10956),
 ('TMAX', 13437),
 ('TMAX_s20', 13437),
 ('TMIN_s20', 13442),
 ('TMIN', 13442)]

#-----------------------------------------------------------------------------

sqlContext.registerDataFrameAsTable(df,'weather') #using older sqlContext instead of newer (V2.0) sparkSession

query="""
SELECT measurement,COUNT(measurement) AS count,
                   MIN(year) AS MinYear 
FROM weather  
GROUP BY measurement 
ORDER BY count
"""
print(query)
sqlContext.sql(query).show()

+-----------+-----+-------+
|measurement|count|MinYear|
+-----------+-----+-------+
|       TOBS|10956|   1876|
|   TOBS_s20|10956|   1876|
|       TMAX|13437|   1873|
|   TMAX_s20|13437|   1873|
|   TMIN_s20|13442|   1873|
|       TMIN|13442|   1873|
|   SNWD_s20|14617|   1888|
|       SNWD|14617|   1888|
|   SNOW_s20|15629|   1884|
|       SNOW|15629|   1884|
|   PRCP_s20|16118|   1871|
|       PRCP|16118|   1871|
+-----------+-----+-------+

#-----------------------------------------------------------------------------

"""
Performing a map command
In order to perform a map on a dataframe, you first need to transform it into an RDD.

Not the recommended way. Better way is to use built-in sparkSQL functions.
Or register new ones (Advanced).
"""

df.rdd.map(lambda row:(row.longitude,row.latitude)).take(5)
Out[4]: 
[(-77.71330261230469, 42.57080078125),
 (-77.71330261230469, 42.57080078125),
 (-77.71330261230469, 42.57080078125),
 (-77.71330261230469, 42.57080078125),
 (-77.71330261230469, 42.57080078125)]

#-----------------------------------------------------------------------------

"""
Slide Type
Slide
Aggregations
Aggregation can be used, in combination with built-in sparkSQL functions to compute statistics of a dataframe.
computation will be fast thanks to combined optimzations with database operations.

A partial list : count(), approx_count_distinct(), avg(), max(), min()

Of these, the interesting one is approx_count_distinct() which uses sampling to get an approximate count fast.

https://spark.apache.org/docs/2.2.0/api/python/_modules/pyspark/sql/functions.html
"""

import pyspark.sql.functions as F # used here just for show.

df.agg({'station':'approx_count_distinct'}).show()

+------------------------------+
|approx_count_distinct(station)|
+------------------------------+
|                           339|
+------------------------------+

#-----------------------------------------------------------------------------

"""
Approximate Quantile
Suppose we want to partition the years into 10 ranges
such that in each range we have approximately the same number of records.
The method .approxQuantile will use a sample to do this for us.
"""

print('with accuracy 0.1: ',df.approxQuantile('year', [0.1*i for i in range(1,10)], 0.1))
# with accuracy 0.1:  [1871.0, 1926.0, 1947.0, 1957.0, 1958.0, 1966.0, 1979.0, 1985.0, 2013.0]

print('with accuracy 0.01: ',df.approxQuantile('year', [0.1*i for i in range(1,10)], 0.01))
# with accuracy 0.01:  [1917.0, 1936.0, 1948.0, 1957.0, 1965.0, 1974.0, 1983.0, 1992.0, 2002.0]

#-----------------------------------------------------------------------------


"""
Lets collect the exact number of rows for each year
This will take much longer than ApproxQuantile on a large file
"""

# Lets collect the exact number of rows for each year ()
query='SELECT year,COUNT(year) AS count FROM weather GROUP BY year ORDER BY year'
print(query)
counts=sqlContext.sql(query)
print('counts is ',counts)


import pandas as pd    
A=counts.toPandas() # Transform a spark Dataframe to a Pandas Dataframe
A.plot.line('year','count')
grid()

#-----------------------------------------------------------------------------

"""
Slide Type
Sub-Slide
Reading rows selectively
Suppose we are only interested in snow measurements. We can apply an SQL query directly to the parquet files. As the data is organized in columnar structure, we can do the selection efficiently without loading the whole file to memory.

Here the file is small, but in real applications it can consist of hundreds of millions of records. In such cases loading the data first to memory and then filtering it is very wasteful.
"""

query="""SELECT station,measurement,year 
FROM parquet.`%s.parquet` 
WHERE measurement=\"SNOW\" """%(data_dir+'/NY')
print(query)
df2 = sqlContext.sql(query)
print(df2.count(),df2.columns)
df2.show(5)

+-----------+-----------+----+
|    station|measurement|year|
+-----------+-----------+----+
|USC00308600|       SNOW|1932|
|USC00308600|       SNOW|1956|
|USC00308600|       SNOW|1957|
|USC00308600|       SNOW|1958|
|USC00308600|       SNOW|1959|
+-----------+-----------+----+
only showing top 5 rows

#-----------------------------------------------------------------------------

"""
Summary
Dataframes can be manipulated decleratively, which allows for more optimization.
Dataframes can be stored and retrieved from Parquet files.
It is possible to refer directly to a parquet file in an SQL query.
"""
