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

import numpy as np
import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark import SparkContext,SparkConf

#-----------------------------------------------------------------------------

def create_sc(pyFiles):
    sc_conf = SparkConf()
    sc_conf.setAppName("Weather_PCA")
    sc_conf.set('spark.executor.memory', '3g')
    sc_conf.set('spark.executor.cores', '1')
    sc_conf.set('spark.cores.max', '4')
    sc_conf.set('spark.logConf', True)
    print(sc_conf.getAll())

    sc = SparkContext(conf=sc_conf,pyFiles=pyFiles)

    return sc 

    sc_conf.set('spark.default.parallelism','10')


sc = create_sc(pyFiles=['lib/numpy_pack.py','lib/spark_PCA.py','lib/computeStatistics.py'])

#-----------------------------------------------------------------------------

from pyspark.sql import *
sqlContext = SQLContext(sc)

import numpy as np
from computeStatistics import *

#-----------------------------------------------------------------------------

state='NY'
data_dir='../resource/asnlib/publicdata/Data'
parquet=state+'.parquet'
parquet_path = data_dir+'/'+parquet


%%time
df=sqlContext.read.parquet(parquet_path)
print(df.count())
df.show(5)


168398
+-----------+-----------+----+--------------------+-----+
|    Station|Measurement|Year|              Values|state|
+-----------+-----------+----+--------------------+-----+
|USW00094704|   PRCP_s20|1945|[00 00 00 00 00 0...|   NY|
|USW00094704|   PRCP_s20|1946|[99 46 52 46 0B 4...|   NY|
|USW00094704|   PRCP_s20|1947|[79 4C 75 4C 8F 4...|   NY|
|USW00094704|   PRCP_s20|1948|[72 48 7A 48 85 4...|   NY|
|USW00094704|   PRCP_s20|1949|[BB 49 BC 49 BD 4...|   NY|
+-----------+-----------+----+--------------------+-----+
only showing top 5 rows

CPU times: total: 0 ns
Wall time: 4.19 s

#-----------------------------------------------------------------------------

sqlContext.registerDataFrameAsTable(df,'table')

Query="""
SELECT Measurement,count(Measurement) as count 
FROM table 
GROUP BY Measurement
ORDER BY count
"""
counts=sqlContext.sql(Query)
counts.show()

+-----------+-----+
|Measurement|count|
+-----------+-----+
|   TOBS_s20|10956|
|       TOBS|10956|
|   TMAX_s20|13437|
|       TMAX|13437|
|   TMIN_s20|13442|
|       TMIN|13442|
|   SNWD_s20|14617|
|       SNWD|14617|
|   SNOW_s20|15629|
|       SNOW|15629|
|   PRCP_s20|16118|
|       PRCP|16118|
+-----------+-----+

#-----------------------------------------------------------------------------

from time import time
t=time()

N=sc.defaultParallelism
print('Number of executors=',N)
print('took',time()-t,'seconds')

Number of executors= 20
took 0.000982522964477539 seconds

#-----------------------------------------------------------------------------

%%time 
### This is the main cell, where all of the statistics are computed.
STAT=computeStatistics(sqlContext,df)


print("   Name  \t                 Description             \t  Size")
print("-"*80)
print('\n'.join(["%10s\t%40s\t%s"%(s[0],s[1],str(s[2])) for s in STAT_Descriptions]))

TMAX : shape of mdf is  13437
time for TMAX is 68.616779088974
SNOW : shape of mdf is  15629
time for SNOW is 70.73119330406189
SNWD : shape of mdf is  14617
time for SNWD is 62.7271990776062
TMIN : shape of mdf is  13442
time for TMIN is 62.90683436393738
PRCP : shape of mdf is  16118
time for PRCP is 69.86481618881226
TOBS : shape of mdf is  10956
time for TOBS is 59.934332847595215
CPU times: total: 266 ms
Wall time: 6min 34s

print("   Name  \t                 Description             \t  Size")
print("-"*80)
print('\n'.join(["%10s\t%40s\t%s"%(s[0],s[1],str(s[2])) for s in STAT_Descriptions]))

   Name  	                 Description             	  Size                      
--------------------------------------------------------------------------------
SortedVals	                        Sample of values	vector whose length varies between measurements
     UnDef	      sample of number of undefs per row	vector whose length varies between measurements
      mean	                              mean value	()
       std	                                     std	()
    low100	                               bottom 1%	()
   high100	                                  top 1%	()
   low1000	                             bottom 0.1%	()
  high1000	                                top 0.1%	()
         E	                   Sum of values per day	(365,)
        NE	                 count of values per day	(365,)
      Mean	                                    E/NE	(365,)
         O	                   Sum of outer products	(365, 365)
        NO	               counts for outer products	(365, 365)
       Cov	                                    O/NO	(365, 365)
       Var	  The variance per day = diagonal of Cov	(365,)
    eigval	                        PCA eigen-values	(365,)
    eigvec	                       PCA eigen-vectors	(365, 365)

#-----------------------------------------------------------------------------