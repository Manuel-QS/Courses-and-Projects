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
from pyspark.sql.functions import udf
%pylab inline

# Just like using Spark requires having a SparkContext, using SQL requires an SQLContext
sqlContext = SQLContext(sc)


#-----------------------------------------------------------------------------

"""
Count the number of records for each measurment
"""

data_dir=r"k-Basics/resource/asnlib/publicdata/Data"

%%time
Query="""
SELECT measurement,COUNT(measurement) AS Count
FROM parquet.`{}`
GROUP BY measurement 
""".format(data_dir + '/US_state_weather.parquet')


Counts_pdf = sqlContext.sql(Query).toPandas()
CPU times: total: 0 ns
Wall time: 4.81 s


Counts_pdf.shape
Out[5]: (133, 2)


Counts_pdf.sort_values('Count', ascending=False).iloc[:7].reset_index(drop=True)

Out[6]: 
  measurement    Count
0        PRCP  1171396
1        SNOW  1051466
2        SNWD   899553
3        TMAX   687240
4        TMIN   687227
5        TOBS   528688
6        WT03   295366

#-----------------------------------------------------------------------------

# Select the 30 most common measurements

%%time
top30=list(Counts_pdf.sort_values('Count', ascending=False).iloc[:30]['measurement'])
top30_str = 'measurement=\'{}\'\n'.format(top30[0])
for m in top30[1:]:
    top30_str += "\tor measurement=\'{}\'\n".format(m)

Query="""
SELECT *
FROM parquet.`{}`
WHERE ({})
""".format(data_dir + '/US_state_weather.parquet', top30_str)
print(Query)
Weather_df = sqlContext.sql(Query)
print('number of records=', Weather_df.count())


WHERE (measurement='PRCP'
	or measurement='SNOW'
	or measurement='SNWD'
	or measurement='TMAX'
	or measurement='TMIN'
	or measurement='TOBS'
	or measurement='WT03'
	or measurement='MDPR'
	or measurement='WT01'
	or measurement='DAPR'
	or measurement='WT04'
	or measurement='WT05'
	or measurement='WT11'
	or measurement='WT06'
	or measurement='WESD'
	or measurement='WESF'
	or measurement='WT16'
	or measurement='TAVG'
	or measurement='WT08'
	or measurement='WT18'
	or measurement='WT14'
	or measurement='PGTM'
	or measurement='WT09'
	or measurement='WT02'
	or measurement='AWND'
	or measurement='EVAP'
	or measurement='WDMV'
	or measurement='WDF2'
	or measurement='WSF2'
	or measurement='WSF5'
)

number of records= 7410400
CPU times: total: 0 ns
Wall time: 1.24 s


#-----------------------------------------------------------------------------

"""
Count the number of undefined days
Each record has a vector of 365 days. Some of the netries are undefined.
We want to know the distribution of the number of undefined entries, grouped by measurement type.
"""
"""
Using User Defined Functions (UDF)
define a python function that operats on the Values field.
"""

def Count_nan(V):
    A=unpackArray(V,data_type=np.float16)
    return int(sum(np.isnan(A)))


# define and register the function as a UDF
Count_nan_udf = udf(Count_nan,IntegerType())


#apply the UDF and create a new column
Weather_df=Weather_df.withColumn("nan_no", Count_nan_udf(Weather_df.Values))



def unpackArray(x,data_type=np.int16):
    return np.frombuffer(x,dtype=data_type)

def Count_nan(V):
    A=unpackArray(V,data_type=np.float16)
    return int(sum(np.isnan(A)))

Count_nan_udf = udf(Count_nan, IntegerType())
Weather_df=Weather_df.withColumn("nan_no", Count_nan_udf(Weather_df.Values))


nan_rdd = Weather_df.rdd

"""
Histograms Using map and reduce
We map each count (1-365) to a vector of 365 zeros with a single 1 at location "count"
We create a histogram by adding the vectors.(reduceByKey())
"""

def map_to_hist(n):
    a=np.zeros(367)
    a[n]=1
    return a
by_measurement=nan_rdd.map(lambda row: (row.Measurement,map_to_hist(row.nan_no)))

#-----------------------------------------------------------------------------

%%time
Hists=by_measurement.reduceByKey(lambda x,y:x+y).collect()

CPU times: total: 0 ns
Wall time: 1min 10s

#-----------------------------------------------------------------------------

# Plotting histograms
hists_dict = dict(Hists)

fig, axes = subplots(6, 5, figsize=(20, 20))
for i, name in enumerate(top30):
    row = i // 5
    col = i % 5
    ax = axes[row][col]
    
    ax.plot(hists_dict[name])
    ax.set_title(name)
    ax.set_xticks([0, 100, 200, 300])
    ax.grid()



"""
Summary

Before analyzing a large dataset, it is important to know how dense it is.
The most common measurement PRCP has 1.5 Million records.
Measurements at location >=30 have fewer than 1,500 records.
Of the 30 most common measurements, 20 have fewer than 60 defined value per year
"""