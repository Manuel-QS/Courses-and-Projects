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

from time import time
import numpy as np
%pylab inline
from matplotlib.backends.backend_pdf import PdfPages
from os.path import isfile,isdir
import os

!pip install lib --upgrade pip
pip install --upgrade pip
import lib
from lib.measureRandomAccess import measureRandomAccess
from lib.PlotTime import PlotTime
from lib.create_file import create_file

#-----------------------------------------------------------------------------

# Measuring memory latency
"""
Goal 1: Measure the effects of caching in the wild
Goal 2: Undestand how to study long-tail distributions.
"""

"""
Latency is the time difference between the time at which the CPU is issuing a read or write command and, 
the time the command is complete.

This time is very short if the element is already in the L1 Cache,
and is very long if the element is in external memory (disk or SSD).
"""

"""
We test access to elements arrays whose sizes are:
m_legend=['Zero','1KB','1MB','1GB','10GB']
Arrays are stored in memory or on disk

We perform 100,000 read/write ops to random locations in the array.
We analyze the distribution of the latencies as a function of the size of the array.
"""


# include 10GB here
# m_list=[0]+[int(10**i) for i in [3,6,9,10]]
# m_legend=['Zero','1KB','1MB','1GB','10GB']

m_list=[0]+[int(10**i) for i in [3,6,9]]
m_legend=['Zero','1KB','1MB','1GB']
L=len(m_list)
k=100000 # number of pokes
print('m_list=',m_list)

"""
>>>
m_list= [0, 1000, 1000000, 1000000000]
"""


"""
Set working directory
This script generates large files.
We put these files in a separate directory so it is easier to delete them later.
"""


## Remember the path for home and  log directories
home_base,=!pwd
log_root=home_base+'/Outputs'
os.makedirs(log_root, exist_ok=True)



_mean=zeros([2,L])   #0: using disk, 1: using memory
_std=zeros([2,L])
_block_no=zeros([L])
_block_size=zeros([L])
T=zeros([2,L,k])



TimeStamp=str(int(time.time()))
log_dir=log_root+'/'+TimeStamp
os.makedirs(log_dir,exist_ok=True)

Random_pokes=[]
Min_Block_size=1000000
for m_i in range(len(m_list)):
    
    m=m_list[m_i]
    blocks=int(m/Min_Block_size)
    if blocks==0:
        _block_size[m_i]=1
        _block_no[m_i]=m
    else:
        _block_size[m_i]=Min_Block_size
        _block_no[m_i]=blocks
    (t_mem,t_disk) = create_file(int(_block_size[m_i]),int(_block_no[m_i]),filename=log_dir+'/'+'BlockData'+str(m))

    (_mean[0,m_i],_std[0,m_i],T[0,m_i]) = measureRandomAccess(m,filename=log_dir+'/'+'BlockData'+str(m),k=k)
    T[0,m_i]=sorted(T[0,m_i])
    print('\rFile pokes _mean='+str(_mean[0,m_i])+', file _std='+str(_std[0,m_i]))

    (_mean[1,m_i],_std[1,m_i],T[1,m_i]) = measureRandomAccess(m,filename='',k=k)
    T[1,m_i]=sorted(T[1,m_i])
    print('\rMemory pokes _mean='+str(_mean[1,m_i])+', Memory _std='+str(_std[1,m_i]))
    
    Random_pokes.append({'m_i':m_i,
                        'm':m,
                        'memory__mean': _mean[1,m_i],
                        'memory__std': _std[1,m_i],
                        'memory_largest': T[1,m_i][-1000:],
                        'file__mean': _mean[0,m_i],
                        'file__std': _std[0,m_i],
                        'file_largest': T[0,m_i][-1000:]                
                })
print('='*50)

!rm -rf $log_dir


"""
>>>
creating 1 byte block: 0.000002 sec, writing 0 blocks 0.008402 sec
File pokes _mean=1.096177101135254e-07, file _std=5.623273681642107e-07
Memory pokes _mean=1.065826416015625e-07, Memory _std=1.3619007639566994e-07
              
creating 1 byte block: 0.000002 sec, writing 1000 blocks 0.011912 sec
File pokes _mean=1.4598691463470459e-05, file _std=6.157602877114688e-05
Memory pokes _mean=1.5909194946289062e-07, Memory _std=3.3596700601393417e-07
              
creating 1000000 byte block: 0.000083 sec, writing 1 blocks 0.078826 sec
File pokes _mean=1.4377799034118652e-05, file _std=5.741696218865172e-05
Memory pokes _mean=2.050614356994629e-07, Memory _std=1.3610647261563586e-06
              
creating 1000000 byte block: 0.000057 sec, writing 1000 blocks 45.481346 sec
File pokes _mean=0.003619978959560394, file _std=0.02507588314034115
Memory pokes _mean=4.873180389404297e-07, Memory _std=1.4951871088652702e-05
==================================================
"""


#------------------------------------------------------------------------------


fields=['m', 'memory__mean', 'memory__std','file__mean','file__std']
print('| block size | mem mean  | mem std | disk mean | disk std |')
print('| :--------- | :----------- | :--- | :-------- | :------- |')
for R in Random_pokes:
    tup=tuple([R[f] for f in fields])
    print('| %d | %6.3g | %6.3g |  %6.3g | %6.3g |'%tup)


"""
>>>
| block size | mem mean  | mem std | disk mean | disk std |
| :--------- | :----------- | :--- | :-------- | :------- |
| 0 | 1.07e-07 | 1.36e-07 |  1.1e-07 | 5.62e-07 |
| 1000 | 1.59e-07 | 3.36e-07 |  1.46e-05 | 6.16e-05 |
| 1000000 | 2.05e-07 | 1.36e-06 |  1.44e-05 | 5.74e-05 |
| 1000000000 | 4.87e-07 | 1.5e-05 |  0.00362 | 0.0251 |
"""




#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------


"""
Characterize sequential access
Random access degrades rapidly with the size of the block.
Sequential access is much faster.
We already saw that writing 10GB to disk sequentially takes 8.9sec, or less than 1 second for a gigbyte.
Writing a 1TB disk at this rate would take ~1000 seconds or about 16 minutes.
"""


Consec=[]
Line='### Consecutive Memory writes:'
print(Line)
n=1000
r=np.array(list(range(n)))
Header="""
|   size (MB) | Average time per byte |
| ---------: | --------------: | """
print(Header)
# for m in [1,1000,1000000,10000000]: # include 10GB experiment
for m in [1,1000,1000000]:
    t1=time.time()
    A=np.repeat(r,m)
    t2=time.time()
    Consec.append((n,m,float(n*m)/1000000,(t2-t1)/float(n*m)))
    print("| %6.3f | %4.2g |" % (float(n*m)/1000000,(t2-t1)/float(n*m)))
A=[];r=[]


"""
>>>
### Consecutive Memory writes:

|   size (MB) | Average time per byte |
| ---------: | --------------: | 
|  0.001 | 3e-08 |
|  1.000 | 8.9e-09 |
| 1000.000 | 9.6e-09 |
"""

#------------------------------------------------------------------------------

"""
# Consecutive Memory writes:

We are measuring bandwidth rather than latency:
We say that it take 8.9sec to write 10GB to SSD, we are NOT saying that to write one byte to SSD it take  
8.9×10−10 second to write a single byte.
This is because many write operations are occuring in parallel.

Byte-rate for writing large blocks is about (100MB/sec)
Byte-rate for writing large SSD blocks is about (1GB/sec)

Comparison:
Memory: Sequential access: 100M/sec, random access:  10−9
 sec for 10KB,  10−6−10−3
  for 10GB
SSD: Sequential access: 1GB/sec, random access:  10−5−10−3
 sec for 10KB,  10−4−10−1
  for 10GB
"""

#------------------------------------------------------------------------------

"""
Collecting System Description
"""

brand_name = "brand: Windows"

if brand_name =="brand: Windows":
    os_release  = !ver
    os_type     = !WMIC CPU get  SystemCreationClassName
    memory      = !WMIC ComputerSystem get TotalPhysicalMemory
    os_info     = os_release + os_type

    cpu_core_count  = !WMIC CPU get NumberOfCores
    cpu_speed       = !WMIC CPU get CurrentClockSpeed
    cpu_model_name  = !WMIC CPU get name
    cpu_info        = cpu_core_count + cpu_speed + cpu_model_name

    l2cachesize = !WMIC CPU get L2CacheSize
    l3cachesize = !WMIC CPU get L3CacheSize
    cache_info  = l2cachesize + l3cachesize


description=[brand_name] + os_info + cache_info + cpu_info
print("Main Harware Parameters:\n")
print('\n'.join(description))

os_type
Out[49]: ['SystemCreationClassName  ', '', 'Win32_ComputerSystem     ', '', '', '']
memory
Out[50]: ['TotalPhysicalMemory  ', '', '68374241280          ', '', '', '']
cpu_core_count
Out[51]: ['NumberOfCores  ', '', '14             ', '', '', '']
cpu_speed
Out[52]: ['CurrentClockSpeed  ', '', '2300               ', '', '', '']
cpu_model_name
Out[53]: 
['Name','','12th Gen Intel(R) Core(TM) i7-12700H  ','','','']
l2cachesize
Out[54]: ['L2CacheSize  ', '', '11776        ', '', '', '']
l3cachesize
Out[55]: ['L3CacheSize  ', '', '24576        ', '', '', '']
