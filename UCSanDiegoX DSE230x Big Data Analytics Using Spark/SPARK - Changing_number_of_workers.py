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

from time import time
from pyspark import SparkContext

#-----------------------------------------------------------------------------

for j in range(1,10):
    sc = SparkContext(master="local[%d]"%(j))
    t0=time()
    for i in range(10):
        sc.parallelize([1,2]*1000000).reduce(lambda x,y:x+y)
    print("%2d executors, time=%4.3f"%(j,time()-t0))
    sc.stop()

 1 executors, time=11.705
 2 executors, time=15.253
 3 executors, time=20.586
 4 executors, time=25.270
 5 executors, time=31.150
 6 executors, time=37.659
 7 executors, time=42.955
 8 executors, time=48.285
 9 executors, time=54.034


"""
It is counterintuitive that increasing the number of executors results in increased execution time. 
The main reasons for this could be:

1. Increasing the number of executors introduces more overhead in terms of task scheduling and communication between 
the driver and executors. This overhead can outweigh the benefits of parallelization, especially for smaller tasks or datasets.

2. More executors can increase the frequency of garbage collection, 
especially if the tasks generate a lot of intermediate data, leading to higher overall execution time.
"""
 
#-----------------------------------------------------------------------------

"""
Getting insights into CPU and memory usage during the execution of Spark job
"""

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def monitor_resources():
    cpu_percent = psutil.cpu_percent(interval=1)
    mem_info = psutil.virtual_memory()
    return cpu_percent, mem_info.percent

for j in range(1, 10):
    sc = SparkContext(master="local[%d]" % j)
    t0 = time()
    cpu_usages = []
    mem_usages = []
    for i in range(10):
        sc.parallelize([1, 2] * 1000000).reduce(lambda x, y: x + y)
        cpu_percent, mem_percent = monitor_resources()
        cpu_usages.append(cpu_percent)
        mem_usages.append(mem_percent)
    elapsed_time = time() - t0
    avg_cpu = sum(cpu_usages) / len(cpu_usages)
    avg_mem = sum(mem_usages) / len(mem_usages)
    print("%2d executors, time=%.3f, avg CPU=%.2f%%, avg MEM=%.2f%%" % (j, elapsed_time, avg_cpu, avg_mem))
    sc.stop()

 1 executors, time=20.877, avg CPU=1.20%, avg MEM=24.87%
 2 executors, time=25.181, avg CPU=1.42%, avg MEM=24.91%
 3 executors, time=30.799, avg CPU=1.70%, avg MEM=24.90%
 4 executors, time=35.961, avg CPU=2.16%, avg MEM=25.10%
 5 executors, time=41.422, avg CPU=1.36%, avg MEM=25.33%
 6 executors, time=47.311, avg CPU=1.34%, avg MEM=25.24%
 7 executors, time=53.604, avg CPU=2.02%, avg MEM=25.25%
 8 executors, time=58.356, avg CPU=1.33%, avg MEM=25.18%
 9 executors, time=67.197, avg CPU=1.83%, avg MEM=25.45%
 
#-----------------------------------------------------------------------------

"""
Potential Issues 

1. The tasks may be too fine-grained. The overhead of scheduling and running many small tasks might be higher than the actual 
computation time. Increase the size of the dataset or the complexity of the tasks to see if the overhead becomes less significant.

2. The default Spark configuration might not be optimal.
"""



import psutil
from pyspark import SparkConf

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def monitor_resources():
    cpu_percent = psutil.cpu_percent(interval=1)
    mem_info = psutil.virtual_memory()
    return cpu_percent, mem_info.percent

# Spark configuration

# https://spark.apache.org/docs/latest/configuration.html 

conf = SparkConf()
conf.set("spark.driver.memory", "8g") #	Amount of memory to use for the driver process
conf.set("spark.executor.memory", "48g") #Amount of memory to use per executor process
conf.set("spark.executor.cores", "1") # The number of cores to use on each executor.
conf.set("spark.task.cpus", "1") # Number of cores to allocate for each task.	

for j in range(1, 10):
    conf.set("spark.executor.instances", str(j))
    sc = SparkContext(master="local[%d]" % j, conf=conf)
    t0 = time()
    cpu_usages = []
    mem_usages = []
    for i in range(10):
        sc.parallelize([1, 2] * 10000000).reduce(lambda x, y: x + y)
        cpu_percent, mem_percent = monitor_resources()
        cpu_usages.append(cpu_percent)
        mem_usages.append(mem_percent)
    elapsed_time = time() - t0
    avg_cpu = sum(cpu_usages) / len(cpu_usages)
    avg_mem = sum(mem_usages) / len(mem_usages)
    print("%2d executors, time=%.3f, avg CPU=%.2f%%, avg MEM=%.2f%%" % (j, elapsed_time, avg_cpu, avg_mem))
    sc.stop()



 1 executors, time=62.801, avg CPU=2.13%, avg MEM=26.34%
 2 executors, time=59.139, avg CPU=2.64%, avg MEM=27.55%
 3 executors, time=58.814, avg CPU=2.08%, avg MEM=27.59%
 4 executors, time=60.353, avg CPU=1.81%, avg MEM=28.02%
 5 executors, time=64.145, avg CPU=1.63%, avg MEM=28.21%
 6 executors, time=70.220, avg CPU=1.53%, avg MEM=28.33%
 7 executors, time=74.284, avg CPU=1.46%, avg MEM=28.53%
 8 executors, time=79.040, avg CPU=1.91%, avg MEM=28.62%
 9 executors, time=85.625, avg CPU=1.40%, avg MEM=28.58%
 
 
 #-----------------------------------------------------------------------------
 
 """
 Same but increasing the work volume
 """

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def monitor_resources():
    cpu_percent = psutil.cpu_percent(interval=1)
    mem_info = psutil.virtual_memory()
    return cpu_percent, mem_info.percent

# Spark configuration

# https://spark.apache.org/docs/latest/configuration.html 

conf = SparkConf()
conf.set("spark.driver.memory", "8g") #	Amount of memory to use for the driver process
conf.set("spark.executor.memory", "48g") #Amount of memory to use per executor process
conf.set("spark.executor.cores", "1") # The number of cores to use on each executor.
conf.set("spark.task.cpus", "1") # Number of cores to allocate for each task.	

for j in range(1, 10):
    conf.set("spark.executor.instances", str(j))
    sc = SparkContext(master="local[%d]" % j, conf=conf)
    t0 = time()
    cpu_usages = []
    mem_usages = []
    for i in range(10):
        sc.parallelize([1, 2] * 100000000).reduce(lambda x, y: x + y)
        cpu_percent, mem_percent = monitor_resources()
        cpu_usages.append(cpu_percent)
        mem_usages.append(mem_percent)
    elapsed_time = time() - t0
    avg_cpu = sum(cpu_usages) / len(cpu_usages)
    avg_mem = sum(mem_usages) / len(mem_usages)
    print("%2d executors, time=%.3f, avg CPU=%.2f%%, avg MEM=%.2f%%" % (j, elapsed_time, avg_cpu, avg_mem))
    sc.stop()
    
 1 executors, time=506.195, avg CPU=1.45%, avg MEM=33.44%
 2 executors, time=367.911, avg CPU=1.85%, avg MEM=35.35%
 3 executors, time=325.913, avg CPU=3.30%, avg MEM=37.27%
 4 executors, time=304.880, avg CPU=1.66%, avg MEM=34.54%
 5 executors, time=306.395, avg CPU=1.34%, avg MEM=36.80%
 6 executors, time=299.532, avg CPU=1.48%, avg MEM=38.52%
 7 executors, time=290.810, avg CPU=1.84%, avg MEM=38.54%
 8 executors, time=289.469, avg CPU=1.51%, avg MEM=38.48%
 9 executors, time=291.125, avg CPU=1.62%, avg MEM=37.32%
   
"""
Conclusion:

More executors does not necessarly mean less execution time and most important
it depends on how large is the total work to be executed.
Larger the total work, relative less is the execution time when increasing the number of executor.
Increasing the number of executors when the work is realtive small does not reduce the execution time.
"""