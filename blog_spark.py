
# coding: utf-8

# In[7]:

from pyspark import SparkContext, SparkConf
import os
import re


# In[8]:

sc = SparkContext()


# path variable contains apth to blogs

path = "/Users/Manu/BiData assignment/blogs"
rdd = sc.wholeTextFiles(path) 


rdd_filename = rdd.map(lambda x: x[0])


# getting industry rdd

indusrdd = rdd_filename.map(lambda x:x.split('/')).map(lambda x: x[-1]).map(lambda x:x.split('.')[-3])


indusrdd = indusrdd.map(lambda x:(x,1)).groupByKey().map(lambda x:x[0])


# In[14]:

listInuds = indusrdd.collect()
setIndus = set([x.lower() for x in listInuds])


# braodcasting set of industry

broadcastVar = sc.broadcast(setIndus)


# In[17]:

path = "/Users/Manu/BiData assignment/blogs"
rdd2 = sc.wholeTextFiles(path) 


# In[18]:

def replaceChar(s):
    s = s.replace("\r","")
    s = s.replace("\n","")
    s = s.replace("\t","")
    s = s.replace(".","")
    s = ' '.join(s.split())
    return s


# In[19]:

rdd2 = rdd2.map(lambda x:x[1]).map(lambda x:x.replace("<Blog>","")).map(lambda x:x.replace("</Blog>","")).map(replaceChar)



rdd3 = rdd2.map(lambda x:' '.join(x.split())).map(lambda x : x.split("</post>")).map(lambda x:[item.replace("<post>","") for item in x]).map(lambda x: [item for item in x if item != ''])


rdd4 = rdd3.map(lambda x: [item.replace("<date>","")for item in x]).map(lambda x: [tuple(item.split("</date>"))for item in x])


# removinf punctuations

import string
def modifyPost(lst):
    res = list()
    for t in lst:
        y,x = t
        for c in string.punctuation:
            if c != '-':
                x = x.replace(c," ")
        x = ' '.join(x.split())
        ylist = y.split(',')
        y = ylist[2]+'-'+ylist[1]
        res.append((y, x.lower().split(" ")))
    return res


# taking out industry names

def intersectionList(lst):
    industryList = broadcastVar.value
    res = list()
    for t in lst:
        words = t[1]
        induswords  = [w for w in words if w in industryList]
        if len(induswords) != 0:
            res.append((t[0],induswords))
    return res



def unfoldList(lst):
    res = list()
    for t in lst:
        yrmonth, indlist = t
        for indus in indlist:
            res.append((indus,yrmonth))
    return res


rdd5 = rdd4.map(modifyPost).map(intersectionList).map(unfoldList).flatMap(lambda x:x).map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b)


rddres = rdd5.map(lambda x:(x[0][0],(x[0][1],x[1]))).sortBy(lambda x: x[1]).groupByKey().mapValues(list)


print (rddres.collect())

