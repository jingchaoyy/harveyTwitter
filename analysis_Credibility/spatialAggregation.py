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
    sql = "select eid, concat(lat, ', ', lng) as coor, tids from " + tb + " where lat is not null"
    print(sql)
    coorList = queryFromDB.freeQuery(dbConnect, sql)

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

    :param setList: e.g. ((437, '38.9029744, -77.0303124', '901611085610016769, 901871542220533760'),
    (570, '28.0427866, -97.0450004', '901291243258425344, 906344003137814529, 904707460770004992, 901297908896505856'))
    :return:
    """
    for set in setList:
        rec1 = set[0]
        rec2 = set[1]
        coor1 = rec1[1].split(',')
        coor2 = rec2[1].split(',')
        dist = location_tools.eucDist(coor1, coor2)
        if dist <= 0.01:
            tids1 = rec1[2]
            tids2 = rec2[2]
            checkTime(tids1, tids2)


def checkTime(tidList1, tidList2):
    """

    :param tidList1:
    :param tidList2:
    :return:
    """
    sql1 = "select tcreate from original where tid in " + "(" + tidList1 + ")"
    sql2 = "select tcreate from original where tid in " + "(" + tidList2 + ")"
    timeSpan1 = queryFromDB.freeQuery(dbConnect, sql1)
    timeSpan2 = queryFromDB.freeQuery(dbConnect, sql2)
    # todo: how to merge after having the two time spans


coors = allCoors(cre_tb)
allSets = findsubsets(coors, 2)  # pair all coordinates fo calculate spatial distance
aggregation = checkDist(allSets)
