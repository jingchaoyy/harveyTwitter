"""
Created on 7/26/2018
@author: Jingchao Yang

Collect tids under certain event and locate their
"""

from psqlOperations import queryFromDB
import matplotlib.pyplot as plt
import pandas as pd

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


def getData(col, eid):
    """
    Input event id from credibility table, and return a list of supporting tids

    :param col: column name
    :param eid: event id
    :return: list of associated data
    """
    tb_out_Name = "original_credibility_improved"
    sql = "select " + col + " from " + tb_out_Name + " where eid = '" + str(eid) + "'"
    data = queryFromDB.freeQuery(dbConnect, sql)[0][0]
    if isinstance(data, str):
        data = data.split(', ')
    print(col, data)
    return data


def dateCount(dList):
    """
    Will simply count twitter numbers for each date

    :param dList: date list
    :return: a pandas df with sorted dates and associated twitter counts
    """
    dateList, count = [], []
    for date in range(len(dList)):
        if dList[date] not in dateList:
            dateList.append(dList[date])
            count.append(1)
        else:
            ind = dateList.index(dList[date])
            count[ind] = count[ind] + 1

    data = {'date': dateList, 'count': count}
    df = pd.DataFrame(data)
    df = df.sort_values('date', ascending=True)
    print(df)
    return df


def dateCredit(dList, locCreitList, rtCreditList):
    """
    Allowing credit accumulation.

    :param dList: date list
    :param locCreitList: list of event match credit
    :param rtCreditList: list of retweet credit
    :return: two pandas dfs, one simply with dates and credits collected for associated days. Another is with evolving
    credits calculated
    """
    dateList, creditList = [], []
    for date in range(len(dList)):
        credit = locCreitList[date] + rtCreditList[date]
        if dList[date] not in dateList:
            dateList.append(dList[date])
            creditList.append(credit)
        else:
            ind = dateList.index(dList[date])
            creditList[ind] = creditList[ind] + credit

    data = {'date': dateList, 'credit': creditList}
    df = pd.DataFrame(data)
    df = df.sort_values('date', ascending=True)
    print(df)

    df_evl = df
    df_evl = df_evl.assign(sum_credit=df_evl.credit.cumsum())
    print(df_evl)

    return df, df_evl


eid = 5536
supTIDs = getData("tids", eid)
timeList = getEvlScore(supTIDs)
dt = pd.to_datetime(timeList)  # from 12h convert to 24h, and using pandas datetime object
dates = dt.date
hours = dt.hour

''' organize by date count'''
count_df = dateCount(dates)

''' organize by credibility '''
supLocCredits = getData("loc_credits", eid)
supRTCredits = getData("rt_credits", eid)
credit_df, credit_df_evl = dateCredit(dates, supLocCredits, supRTCredits)

ax1 = plt.subplot(311)
plt.plot(count_df['date'], count_df['count'])
plt.xticks(rotation='vertical')
plt.title('Twitter Count Plot')
plt.ylabel('Total Tweets Number')
# plt.xlabel('Dates')

ax2 = plt.subplot(312, sharex=ax1)
plt.plot(credit_df['date'], credit_df['credit'])
plt.xticks(rotation='vertical')
plt.title('Credibility Evolving Plot')
plt.ylabel('Credibility Score')
# plt.xlabel('Dates')

ax3 = plt.subplot(313, sharex=ax1)
plt.plot(credit_df_evl['date'], credit_df_evl['sum_credit'])
plt.xticks(rotation='vertical')
plt.title('Credibility Evolving Plot')
plt.ylabel('Credibility Accumulation')
plt.xlabel('Dates')

plt.setp(ax1.get_xticklabels(), visible=False)
plt.setp(ax2.get_xticklabels(), visible=False)
plt.show()
