from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import SQLContext
from operator import add
import re
import pandas as pd
list_index=[]
index=0

def sortlist(x):
    x.sort(reverse=False)
    return x

def data_matching(arr_dict,compare1):
    compare_len=len(compare1)
    for key1 in arr_dict.keys():
        length1=len(compare_len[key])
        if abs(length1-compare_len)>10:
            arr_dict.pop(key1)
    return arr_dict

def itemindex(x):
    global list_index
    global index
    if x not in list_index:
        list_index.append(x)
        index=index+1
        #print(list_index) 
   # return list([x,list_index.index(x)])
    return list_index.index(x)

if __name__ == '__main__':
    sc = SparkContext(appName="inf551")
    d = sc.textFile('a.csv',10)
    header=d.first()
    data=d.filter(lambda row: row!=header).map(lambda x: x.split(','))
    data=data.map(lambda x: (x[2],x[2])).mapValues(lambda x: x.strip())\
        .mapValues(lambda x: x.lower())\
        .mapValues(lambda x: re.sub('[^a-zA-Z0-9]+','',x))\
        .mapValues(lambda x: x.split(' '))\
        .mapValues(lambda x: list(set(x)))\
        .mapValues(lambda x: sortlist(x))\
        .flatMapValues(lambda x: x)\
        .mapValues(lambda x: itemindex(x))
    list_signature=[]
    
    listOfItemset=data.mapValues(lambda x: x).reduceByKey(lambda x,y: min(x,y)).sortByKey(True).map(lambda x: x[0]).collect()
    #print(listOfItemset)
    for i in range(0,50):   #(0,50) 
        signature=data.mapValues(lambda x: (3*int(x)+11*i)%100+1).reduceByKey(lambda x,y: min(x,y)).sortByKey(True).map(lambda x: x[1]).collect()
    #hash function should be fixed
        list_signature.append(signature)
    
    band_signature=[]
    for i in range(0,10):   #(0,10)*5 5 to one band
        begin=i*1
        length=len(list_signature[begin])
        list1=list_signature[begin]
        list2=list_signature[begin+1]
        list3=list_signature[begin+2]
        list4=list_signature[begin+3]
        list5=list_signature[begin+4]
        #list_final=list1
        list_final=list1+list2+list3+list4+list5   #hash function 2
        band_signature.append(list_final)
    
    length=len(listOfItemset)
    

    
    uncluster=[]

    cluster_dict={}
    for i in range(0,length):
        uncluster.append(True)
    for i in range(0,length):
        if uncluster[i]==True:
            similar_list=[]
            for j in range(0,length): #for i j these two itemset compare similarity
                if uncluster[j]==True:
                    rate=0
                    for k in range(0,10):
                        compare=band_signature[k][j]
                        standard=band_signature[k][i]
                        if compare==standard:
                            rate=rate+0.1
                    if rate>=0.8:
                        similar_list.append(j)   
                        uncluster[j]=False   
            #print(similar_list)        
            cluster_dict[i]=similar_list
    #print(cluster_dict)
        #print('Itemset #',facility_name[facility_name['serial_number'].isin([listOfItemset[i]])]['facility_name'].values[0],':',sep='',end='')
            
        #print('Itemset #',listOfItemset[i],':',sep='',end='')
        #for m in similar_list:
            #print(listOfItemset[m],sep=' ',end='')
            #print(facility_name[facility_name['serial_number'].isin([listOfItemset[m]])]['facility_name'].values[0],sep='     ',end='')
                
        #print('')
    print(cluster_dict)
    
    f=open('lsh.txt','w+')
    for key in cluster_dict.keys():
        list_out=[]
        for i in cluster_dict[key]:
            list_out.append(listOfItemset[i])
        arr_dict=dict((x,list_out.count(x)) for x in list_out)
        print(listOfItemset[key],':',sep='',end='',file=f)
        print(arr_dict,sep='',file=f)
        #print(,':  ',sep='',end='',file=f)
        #print(facility_name[facility_name['serial_number'].isin([listOfItemset[key]])]['facility_name'].values[0],':  ',sep='',end='')
        #arr_dict=data_matching(arr_dict,compare)
        #for m in arr_dict.keys():
            #print(m,'(',arr_dict[m],'),  ',sep='',end='',file=f)
            #print(m,'(',arr_dict[m],'),  ',sep='',end='')
        #print('',file=f)
        #print('')
    
    f.close()


