"""
Created on 7/26/2018
@author: Jingchao Yang

Collect tids under certain event and locate their
"""

from psqlOperations import queryFromDB

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"


def getEvlScore(tidList):
    """
    Input a list of supporting tids for certain event from credibility table, and locate their post time in original
    tweet table

    :param tidList: supporting tids
    :return: list of post time
    """
    tb_out_Name = "original"

    timeLine = []
    for tid in tidList:
        sql = "select tcreate from " + tb_out_Name + " where tid = '" + str(tid) + "'"
        pt = queryFromDB.freeQuery(dbConnect, sql)[0][0]
        timeLine.append(pt)
    return timeLine


def getTIDs(eid):
    """
    Input event id from credibility table, and return a list of supporting tids

    :param eid: event id
    :return: list of associated
    """
    tb_out_Name = "original_credibility_improved"
    sql = "select tids from " + tb_out_Name + " where eid = '" + str(eid) + "'"
    tids = queryFromDB.freeQuery(dbConnect, sql)[0][0]
    tids = tids.split(', ')
    print('Max re tweet number:', tids)

    return tids

supTIDs = getTIDs(7)
timeList = getEvlScore(supTIDs)
for i in timeList:
    print(i)