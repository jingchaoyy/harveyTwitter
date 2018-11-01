"""
Created on 11/1/2018
@author: Jingchao Yang

Post direct location merge (the 3-step merge in eventBased.py)
Using power incidents as example. Those events that happened in the same day and within 1km (0.01 degree) will be
aggregated and treated as one incident.
"""
from psqlOperations import queryFromDB
import itertools
from toolBox import location_tools

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
cre_tb = "original_credibility_power3"


def allCoors(tb):
    """

    :param tb:
    :return:
    """
    sql = "select eid, concat(lat, ', ', lng) as coor, tids from " + tb
    print(sql)
    coorList = queryFromDB.freeQuery(dbConnect, sql)[0][0]

    return coorList


def findsubsets(S, k):
    """

    :param S: Set
    :param k: the number of elements in subset
    :return: all combinations
    """
    return set(itertools.combinations(S, k))


def checkDist(setList):
    """

    :param setList:
    :return:
    """
    for set in setList:
        coor1 = set[0]
        coor2 = set[1]
        dist = location_tools.eucDist(coor1, coor2)
        if dist <= 0.01:
            pass


def checkTime(tidList1, tidList2):
    """

    :param tidList1:
    :param tidList2:
    :return:
    """


coors = allCoors(cre_tb)
allSets = findsubsets(coors, 2)  # pair all coordinates fo calculate spatial distance
aggregation = checkDist(allSets)
