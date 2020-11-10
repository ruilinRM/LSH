from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import SQLContext
import numpy as np
import sys

#from math import min

def hash_function(d1,d2,d3,d4,d5,limit):
    return limit**4*d1+limit**3*d2+limit**2*d3+limit*d4+d5


sc=SparkContext(appName="inf553")
sqlContext=SQLContext(sc)
d=sc.textFile('ratings.csv')


header=d.first()
data=d.filter(lambda row: row!=header).map(lambda x: x.split(','))
data_um=data.map(lambda x: (x[0],x[1])).collect()
print(data_um)
max_user=0
max_movie=0
for i in data_um.collect():
    if int(i[0])>max_user:
        max_user=int(i[0])
    if int(i[1])>max_movie:
        max_movie=int(i[1])
limit=max_user+1
new_array=[]
signature=[]
