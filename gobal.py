from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import SQLContext
from operator import add
import re
import pandas as pd
sc = SparkContext(appName="inf551")
d = sc.textFile('a.csv',10)
header=d.first()
data2=d.filter(lambda row: row!=header).map(lambda x: x.split(','))
facility_name=data2.map(lambda x: (x[0],x[2])).collect()
facility_dict={}
for i in facility_name:
    facility_dict[i[0]]=i[1]
print(facility_dict)