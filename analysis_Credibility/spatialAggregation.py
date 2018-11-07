"""
Created on 11/1/2018
@author: Jingchao Yang

Post direct location merge (the 3-step merge in eventBased.py)
Using power incidents as example. Those events that happened within 1km (0.01 degree) will be
aggregated and treated as one incident. The our put from this will be stored separately as index for credibility table,
which can be used as dictionary to match back to credibility table to aggregate info
"""
from psqlOperations import queryFromDB
import itertools
from toolBox import location_tools

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
cre_tb = "original_credibility_power3"


def allData(tb):
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


def aggByDist(setList):
    """

    :param setList: e.g. ((437, '38.9029744, -77.0303124', '901611085610016769, 901871542220533760'),
    (570, '28.0427866, -97.0450004', '901291243258425344, 906344003137814529, 904707460770004992, 901297908896505856'))
    :return: merged EIDs
    """
    mergedEID, eids = [], []
    for set in setList:
        rec1 = set[0]
        rec2 = set[1]
        eid1 = rec1[0]
        eid2 = rec2[0]
        coor1 = rec1[1].split(',')
        coor2 = rec2[1].split(',')
        dist = location_tools.eucDist(coor1, coor2)

        if dist <= 0.01:
            if eid1 not in eids and eid2 not in eids:
                mergedEID.append({eid1, eid2})
                eids.append(eid1)
                eids.append(eid2)
            elif eid1 not in eids and eid2 in eids:
                for merged in mergedEID:
                    if eid2 in merged:
                        merged.add(eid1)
                        eids.append(eid1)
            elif eid1 in eids and eid2 not in eids:
                for merged in mergedEID:
                    if eid1 in merged:
                        merged.add(eid2)
                        eids.append(eid2)
            else:
                continue
    return mergedEID, eids


data = allData(cre_tb)
allIDs = [d[0] for d in data]
allSets = findsubsets(data, 2)  # pair all coordinates fo calculate spatial distance
aggregation, aggIDs = aggByDist(allSets)
otherIDs = set(allIDs) - set(aggIDs)
format = [{i} for i in otherIDs]
resultSet = aggregation + format
