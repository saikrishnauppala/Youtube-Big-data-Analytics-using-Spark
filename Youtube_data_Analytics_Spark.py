#!/usr/bin/env python
# coding: utf-8

# In[11]:


import findspark
findspark.init()
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from operator import add
import math

#Creating Spark Context
sc = SparkContext.getOrCreate()
#Creating Spark Session
spark = SparkSession(sc)
#Readind data set(csv file),dropping null values and selecting only tabs and views column
tags_views = spark.read.csv("file:///C:\\Users\\saikr\\Desktop\\DIC\\USvideos.csv", inferSchema = True, header = True).dropna().select("tags","views")
#Mapping by splitting tags with "|" character,
def tags_split(x):
    tags=x["tags"].split("|")
    result=[]
    for every in tags:
        #Associating view count as counter and applying log on view count because views count will be really large
        if not x["views"].isdigit() or every==None:
            continue        
        result.append((every.strip("\"").lower(),math.log(int(x["views"]))))#Stripping unnecessary characters
    return tuple(result)
rdd1=tags_views.rdd.flatMap(tags_split).reduceByKey(add)#reduce by similar tags and adding its view count
#Top Tags are queried from RDD by Sorting RDD's in descending order of view count
toptags=rdd1.takeOrdered(35, key = lambda x: -x[1])
df=spark.createDataFrame(toptags)
#Writing back to Disk
df.repartition(1).write.csv(path="file:///C:\\Users\\saikr\\Desktop\\DIC\\trending.csv")
#Spark session
spark.stop()

