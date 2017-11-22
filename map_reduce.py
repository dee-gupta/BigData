########################################
## Template Code for Big Data Analytics
## assignment 1 - part I, at Stony Brook Univeristy
## Fall 2017


import sys
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint
import numpy as np
from random import random
import math


##########################################################################
##########################################################################
# PART I. MapReduce

class MyMapReduce:  # [TODO]
    __metaclass__ = ABCMeta

    def __init__(self, data, num_map_tasks=5, num_reduce_tasks=3):  # [DONE]
        self.data = data  # the "file": list of all key value pairs
        self.num_map_tasks = num_map_tasks  # how many processes to spawn as map tasks
        self.num_reduce_tasks = num_reduce_tasks  # " " " as reduce tasks

    ###########################################################   
    # programmer methods (to be overridden by inheriting class)

    @abstractmethod
    def map(self, k, v):  # [DONE]
        print("Need to override map")

    @abstractmethod
    def reduce(self, k, vs):  # [DONE]
        print("Need to override reduce")

    ###########################################################
    # System Code: What the map reduce backend handles

    def mapTask(self, data_chunk, namenode_m2r):  # [DONE]
        # runs the mappers and assigns each k,v to a reduce task
        for (k, v) in data_chunk:
            # run mappers:
            mapped_kvs = self.map(k, v)
            # assign each kv pair to a reducer task
            for (k, v) in mapped_kvs:
                namenode_m2r.append((self.partitionFunction(k), (k, v)))

    def partitionFunction(self, k):  # [TODO]
        # given a key returns the reduce task to send it
        return self.genHash(k)

    def genHash(self, key):

        if type(key) is int or type(key) is float:
            key = int(key)
            return key % self.num_reduce_tasks

        hashcode = 0
        for c in key:
            hashcode += ord(c)
        return hashcode % self.num_reduce_tasks

    def reduceTask(self, kvs, namenode_fromR):  # [TODO]

        # sort all values for each key (can use a list of dictionary)
        # call reducers on each key with a list of values
        # and append the result for each key to namenode_fromR
        # [TODO]
        mapList = dict()

        for (k, v) in kvs:
            try:
                mapList[k].append(v)
            except KeyError:
                mapList[k] = [v]

        for key in mapList:
            mapList[key].sort()
            reduced_kv = self.reduce(key, mapList[key])
            if reduced_kv is not None:
                namenode_fromR.append(reduced_kv)

    def runSystem(self):  # [TODO]
        # runs the full map-reduce system processes on mrObject

        # the following two lists are shared by all processes
        # in order to simulate the communication
        # [DONE]
        namenode_m2r = Manager().list()  # stores the reducer task assignment and
        # each key-value pair returned from mappers
        # in the form: [(reduce_task_num, (k, v)), ...]
        namenode_fromR = Manager().list()  # stores key-value pairs returned from reducers
        # in the form [(k, v), ...]

        # divide up the data into chunks accord to num_map_tasks, launch a new process
        # for each map task, passing the chunk of data to it.
        # hint: if chunk contains the data going to a given maptask then the following
        #      starts a process
        #      p = Process(target=self.mapTask, args=(chunk,namenode_m2r))
        #      p.start()  
        #  (it might be useful to keep the processes in a list)
        # [TODO]

        processList = list()

        ##################################
        index = 0
        chunkLen = int(len(self.data) / self.num_map_tasks)
        safeInd = len(self.data) - chunkLen

        while index < len(self.data):

            hIndex = index + chunkLen
            if hIndex > safeInd:
                chunk = self.data[index:]
                index = len(self.data)

            else:
                chunk = self.data[index:hIndex]
                index = hIndex

            p = Process(target=self.mapTask, args=(chunk, namenode_m2r))
            processList.append(p)
            p.start()

        #####################################



        # index = 0
        # chunkLen = int(len(self.data) / self.num_map_tasks)
        # # print("index %d , chunk len %d" % (index, chunkLen))
        #
        # while index + chunkLen <= len(self.data):
        #     # print("index %d , chunk len %d" % (index, chunkLen))
        #     hIndex = index + chunkLen
        #     if hIndex + chunkLen > len(self.data):
        #         # print("in if")
        #         chunk = self.data[index:]
        #     else:
        #         # print("in else")
        #         chunk = self.data[index:hIndex]
        #     index = hIndex
        #     # print("new index ", index)
        #
        #     p = Process(target=self.mapTask, args=(chunk, namenode_m2r))
        #     processList.append(p)
        #     p.start()

        # join map task processes back
        # [TODO]

        for poc in processList:
            poc.join()

        # print output from map tasks
        # [DONE]
        print("namenode_m2r after map tasks complete:")
        pprint(sorted(list(namenode_m2r)))

        # "send" each key-value pair to its assigned reducer by placing each
        # into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]


        to_reduce_task = [[] for i in range(self.num_reduce_tasks)]

        for i in range(len(namenode_m2r)):
            reducer, kval = namenode_m2r[i]
            to_reduce_task[reducer].append(kval)

        # [TODO]
        procList = list()

        # launch the reduce tasks as a new process for each.
        # [TODO]
        for i in range(len(to_reduce_task)):
            p = Process(target=self.reduceTask, args=(to_reduce_task[i], namenode_fromR))
            procList.append(p)
            p.start()

        # join the reduce tasks back
        # [TODO]

        for poc in procList:
            poc.join()

        # print output from reducer tasks
        # [DONE]
        print("namenode_m2r after reduce tasks complete:")
        pprint(sorted(list(namenode_fromR)))

        # return all key-value pairs:
        # [DONE]
        return namenode_fromR


##########################################################################
##########################################################################
##Map Reducers:

class WordCountMR(MyMapReduce):  # [DONE]
    # the mapper and reducer for word count
    def map(self, k, v):  # [DONE]
        counts = dict()
        for w in v.split():
            w = w.lower()  # makes this case-insensitive
            try:  # try/except KeyError is just a faster way to check if w is in counts:
                counts[w] += 1
            except KeyError:
                counts[w] = 1
        return counts.items()

    def reduce(self, k, vs):  # [DONE]
        return (k, np.sum(vs))

        # contains the map and reduce function for set difference
        # Assume that the mapper receives the "set" as a list of any primitives or comparable objects

        # namenode_m2r list contains( reducerNumber, (k,v) )


class SetDifferenceMR(MyMapReduce):  # [TODO]

    def map(self, k, v):
        finalList = list()

        for val in v:
            finalList.append((val, k))

        return finalList

    def reduce(self, k, vs):
        for v in vs:
            if v == 'S':
                return None

        return k


##########################################################################
##########################################################################


if __name__ == "__main__":  # [DONE: Uncomment pieces to test]
    ###################
    ##run WordCount:
    data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
            (8,
             "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
            (9, "The car raced past the finish line just in time."),
            (10, "Car engines purred and the tires burned.")]
    mrObject = WordCountMR(data, 4, 3)
    mrObject.runSystem()

    ####################
    ##run SetDifference
    # (TODO: uncomment when ready to test)
    print("\n\n*****************\n Set Difference\n*****************\n")
    data1 = [('R', ['apple', 'orange', 'pear', 'blueberry']),
             ('S', ['pear', 'orange', 'strawberry', 'fig', 'tangerine'])]
    data2 = [('R', [x for x in range(50) if random() > 0.5]),
             ('S', [x for x in range(50) if random() > 0.75])]

    print()
    mrObject = SetDifferenceMR(data1, 2, 2)
    mrObject.runSystem()
    mrObject = SetDifferenceMR(data2, 2, 2)
    mrObject.runSystem()
