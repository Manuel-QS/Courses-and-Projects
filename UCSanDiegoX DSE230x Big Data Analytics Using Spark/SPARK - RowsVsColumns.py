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

#-----------------------------------------------------------------------------

# Memory locality, Rows vs. Columns. - experiments with row vs column scanning

def sample_run_times(T,k=10):
    """ compare the time to sum an array row by row vs column by column
        T: the sizes of the matrix, [10**e for e in T] 
        k: the number of repetitions of each experiment
    """
    all_times=[]
    for e in T:
        n=int(10**e)
        #print('\r',n)
        a=np.ones([n,n])
        times=[]

        for i in range(k):    
            t0=time()
            s=0;
            for i in range(n):
                s+=sum(a[:,i])
            t1=time()
            s=0;
            for i in range(n):
                s+=sum(a[i,:])
            t2=time()
            times.append({'row minor':t1-t0,'row major':t2-t1})
        all_times.append({'n':n,'times':times})
    return all_times


#example run
sample_run_times([1,2],k=1)


"""
>>> 
[{'n': 10, 'times': [{'row minor': 0.0, 'row major': 0.0}]},
 {'n': 100, 'times': [{'row minor': 0.0010170936584472656, 'row major': 0.0}]}]
"""

#-----------------------------------------------------------------------------

"""
Plot the ratio between run times as function of n
Here we have small steps between consecutive values of n and only one measurement for each (k=1)
"""

all_times=sample_run_times(np.arange(1.5,3.01,0.001),k=1)

n_list=np.array([a['n'] for a in all_times])
ratios=np.array([a['times'][0]['row minor']/(a['times'][0]['row major']+0.000001) for a in all_times])

# Remove anomaly
valid_indices = ratios < 10
n_list = n_list[valid_indices]
ratios = ratios[valid_indices]

figure(figsize=(15,10))
plot(n_list,ratios)
grid()
xlabel('size of matrix')
ylabel('ratio or running times')
title('time ratio as a function of size of array');

"""
CONCLUSION:
    
- Traversing a numpy array column by column takes more than row by row.
- The effect increasese proportionally to the number of elements in the array (square of the number of rows or columns).
- Run time has large fluctuations.    
"""

#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------

# Quantifying the random fluctuations

k=100
all_times=sample_run_times(np.arange(1,3.001,0.01),k=k)
_n=[]
_row_major_mean=[]
_row_major_std=[]
_row_major_std=[]
_row_minor_mean=[]
_row_minor_std=[]
_row_minor_min=[]
_row_minor_max=[]
_row_major_min=[]
_row_major_max=[]

for times in all_times:
    _n.append(times['n'])
    row_major=[a['row major'] for a in times['times']]
    row_minor=[a['row minor'] for a in times['times']]
    _row_major_mean.append(np.mean(row_major))
    _row_major_std.append(np.std(row_major))
    _row_major_min.append(np.min(row_major))
    _row_major_max.append(np.max(row_major))

    _row_minor_mean.append(np.mean(row_minor))
    _row_minor_std.append(np.std(row_minor))
    _row_minor_min.append(np.min(row_minor))
    _row_minor_max.append(np.max(row_minor))

_row_major_mean=np.array(_row_major_mean)
_row_major_std=np.array(_row_major_std)
_row_minor_mean=np.array(_row_minor_mean)
_row_minor_std=np.array(_row_minor_std)

#-----------------------------------------------------------------------------

figure(figsize=(20,13))
plot(_n,_row_major_mean,'o',label='row major mean')
plot(_n,_row_major_mean-_row_major_std,'x',label='row major mean-std')
plot(_n,_row_major_mean+_row_major_std,'x',label='row major mean+std')
plot(_n,_row_major_min,label='row major min among %d'%k)
plot(_n,_row_major_max,label='row major max among %d'%k)
plot(_n,_row_minor_mean,'o',label='row minor mean')
plot(_n,_row_minor_mean-_row_minor_std,'x',label='row minor mean-std')
plot(_n,_row_minor_mean+_row_minor_std,'x',label='row minor mean+std')
plot(_n,_row_minor_min,label='row minor min among %d'%k)
plot(_n,_row_minor_max,label='row minor max among %d'%k)
xlabel('size of matrix')
ylabel('running time')
legend()
grid()




































