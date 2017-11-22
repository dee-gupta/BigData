
# coding: utf-8

# In[1]:

from pyspark import SparkContext, SparkConf
import os
import string
import re
from random import random
conf = SparkConf().setAppName('blogs-insdustries-filter').set("spark.driver.host", "localhost");
sc = SparkContext(conf=conf)


# In[ ]:

###=============================word count implementation=======================================###

data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
            (8,
             "I believe that at the end of the ,, century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
            (9, "The car raced past the finish line just in time."),
            (10, "Car engines purred and the tires burned.")]

rdd = sc.parallelize(data)
rdd = rdd.map(lambda x: x[1]).        map(lambda s: s.split()).         map(lambda x: [''.join(c for c in s if c not in string.punctuation) for s in x]).         map(lambda x:[item for item in x if item != '']).        flatMap(lambda x: x).         map(lambda x: ' '.join(x.split())).        map(lambda x: x.lower()).         map(lambda x: (x, 1)).         reduceByKey(lambda a, b: a + b)
print (sorted(rdd.collect()))


# In[8]:

###===============================Set diff implementation========================================##
data1 = [('R', ['apple', 'orange', 'pear', 'blueberry']),
             ('S', ['pear', 'orange','strawberry', 'fig', 'tangerine'])]

rdd = sc.parallelize(data1)
rdd = rdd.map(lambda x: [(item, x[0]) for item in x[1]]).         flatMap(lambda x: x).         groupByKey().         filter(lambda x: 'S' not in x[1]).         map(lambda x: x[0])

print(rdd.collect())

data2 = [('R', [x for x in range(50) if random() > 0.5]),
             ('S', [x for x in range(50) if random() > 0.75])]

rdd = sc.parallelize(data2)
rdd = rdd.map(lambda x: [(item, x[0]) for item in x[1]]).         flatMap(lambda x: x).         groupByKey().         filter(lambda x: 'S' not in x[1]).         map(lambda x: x[0])

print (rdd.collect())


# In[ ]:



