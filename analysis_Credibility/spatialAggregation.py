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

# import pandas as pd

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
cre_tb = "original_credibility_power3"
org_tb = "original"


class Event(object):
    # eventID = 0
    locMergeID = 0
    orgID = []
    time = []
    loc_credits = []
    rt_credits = []

    # The class "constructor" - It's actually an initializer
    def __init__(self, locMergeID, orgID, time, loc_credits, rt_credits):
        """

        :param locMergeID: newly generated id after gazetteer/ event merged based on location
        :param orgID: list of original gazetteer/ event id in gazetteer table
        :param time: posting time list
        :param loc_credits: list contain location credibility for each supporting tweet
        :param rt_credits: list contain retweet credibility for each supporting tweet
        """
        # self.eventID = eventID
        self.locMergeID = locMergeID
        self.orgID = orgID
        self.time = time
        self.loc_credits = loc_credits
        self.rt_credits = rt_credits


def getMaxRT(tidList):
    """

    :param tb:
    :return:
    """
    tids = ', '.join(tidList)
    sql = "select max(t_recount) from original where tid in (" + tids + ")"
    result = queryFromDB.freeQuery(dbConnect, sql)[0][0]
    print('Max re tweet number:', result)

    return result


def getPosttime(tidList, tb):
    """
    Input a list of supporting tids for certain event from credibility table, and locate their post time in original
    tweet table
    :param tidList: supporting tids
    :return: list of post time
    """

    timeLine = []
    for tid in tidList:
        sql = "select tcreate from " + tb + " where tid = '" + str(tid) + "'"
        pt = queryFromDB.freeQuery(dbConnect, sql)[0][0]
        timeLine.append(pt)
    return timeLine


def allCoor(tb):
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


def sepByTime(locmerge):
    """

    :param locmerge: e.g (0, {463, 207})
    :return:
    """
    locMergeID = locmerge[0]
    orgIDS = locmerge[1]
    sep, sepDate = [], []
    for oid in orgIDS:
        data = queryFromDB.get_multiColData_where(dbConnect, cre_tb, ["tids", "loc_credits", "rt_credits"], oid)
        supTIDs = data[0].split(', ')
        loc_credits = data[1]
        rt_credits = data[2]
        timeList = getPosttime(supTIDs, org_tb)
        # dt = pd.to_datetime(timeList)  # from 12h convert to 24h, and using pandas datetime object
        for t in range(len(timeList)):
            date = timeList[t].split(' ')[0]
            loc_credit = loc_credits[t]
            rt_credit = rt_credits[t]
            if date not in sepDate:
                event = Event(locMergeID, [oid], [timeList[t]], [loc_credit], [rt_credit])
                sepDate.append(date)
                sep.append(event)
            else:
                ind = sepDate.index(date)
                sep[ind].time.append(timeList[t])
                sep[ind].loc_credits.append(loc_credit)
                sep[ind].rt_credits.append(rt_credit)
                if oid not in sep[ind].orgID:
                    sep[ind].orgID.append(oid)

    return sep


data = allCoor(cre_tb)
allIDs = [d[0] for d in data]

'''Get all tids and remove duplicates'''
allTIDs = [d[-1] for d in data]
allTIDs = ', '.join(allTIDs)
allTIDList = allTIDs.split(', ')
allTIDSet = set(allTIDList)
allTIDList = list(allTIDSet)

maxRT = getMaxRT(allTIDList)
allSets = findsubsets(data, 2)  # pair all coordinates fo calculate spatial distance
aggregation, aggIDs = aggByDist(allSets)
otherIDs = set(allIDs) - set(aggIDs)
format = [{i} for i in otherIDs]
mergedSet = aggregation + format
resultSet = []
for merge in range(len(mergedSet)):
    mergedSet[merge] = (merge, mergedSet[merge])  # e.g. (0, {463, 207})
    result = sepByTime(mergedSet[merge])
    resultSet = resultSet + result
# for i in resultSet:
#     print(i.locMergeID, i.orgID, i.time, i.loc_credits, i.rt_credits)
