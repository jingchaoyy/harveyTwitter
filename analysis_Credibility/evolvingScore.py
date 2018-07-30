"""
Created on 7/26/2018
@author: Jingchao Yang

Collect tids under certain event and locate their
"""

from psqlOperations import queryFromDB
import matplotlib.pyplot as plt
import pandas as pd
import datetime

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
    print('Supporting tweet IDs:', tids)
    return tids


def dateCount(dList):
    dateList, count = [], []
    for date in dList:
        if date not in dateList:
            dateList.append(date)
            count.append(1)
        else:
            ind = dateList.index(date)
            count[ind] = count[ind] + 1

    data = {'date': dateList, 'count': count}
    df = pd.DataFrame(data)
    df = df.sort_values('date', ascending=True)
    print(df)
    plt.plot(df['date'], df['count'])
    plt.xticks(rotation='vertical')
    plt.show()


supTIDs = getTIDs(13)
timeList = getEvlScore(supTIDs)
dt = pd.to_datetime(timeList)  # from 12h convert to 24h, and using pandas datetime object
dates = dt.date
hours = dt.hour

''' Plot '''
# dateCount(dates)  # plot by date count
