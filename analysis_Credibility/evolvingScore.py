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
    dateList, creditList, cl_loc, cl_rt = [], [], [], []
    for date in range(len(dList)):
        credit = locCreitList[date] + rtCreditList[date]
        c_loc = locCreitList[date]
        c_rt = rtCreditList[date]
        if dList[date] not in dateList:
            dateList.append(dList[date])
            creditList.append(credit)
            cl_loc.append(c_loc)
            cl_rt.append(c_rt)
        else:
            ind = dateList.index(dList[date])
            creditList[ind] = creditList[ind] + credit
            cl_loc[ind] = cl_loc[ind] + c_loc
            cl_rt[ind] = cl_rt[ind] + c_rt

    ''' Merged credibility score'''
    data = {'date': dateList, 'credit': creditList}
    df = pd.DataFrame(data)
    df = df.sort_values('date', ascending=True)
    print(df)

    df_evl = df
    df_evl = df_evl.assign(sum_credit=df_evl.credit.cumsum())
    print(df_evl)

    ''' Event match credibility score'''
    data_loc = {'date': dateList, 'credit': cl_loc}
    df_loc = pd.DataFrame(data_loc)
    df_loc = df_loc.sort_values('date', ascending=True)
    df_evl_loc = df_loc
    df_evl_loc = df_evl_loc.assign(sum_credit=df_evl_loc.credit.cumsum())

    ''' Retweet credibility score'''
    data_rt = {'date': dateList, 'credit': cl_rt}
    df_rt = pd.DataFrame(data_rt)
    df_rt = df_rt.sort_values('date', ascending=True)
    df_evl_rt = df_rt
    df_evl_rt = df_evl_rt.assign(sum_credit=df_evl_rt.credit.cumsum())

    return df, df_evl, df_loc, df_evl_loc, df_rt, df_evl_rt


eid = 5536  # original_credibility_damage(6904 high_high); original_credibility_improved (5562 low_high, 5536 high_high)
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
credit_df, credit_df_evl, credit_df_loc, credit_df_evl_loc, credit_df_rt, credit_df_evl_rt \
    = dateCredit(dates, supLocCredits, supRTCredits)

''' Paper Section 4.3.1 '''
fig, (ax0, ax2, ax4) = plt.subplots(3, sharey=True)

color = 'tab:red'
# ax0.set_xlabel('Dates')
ax0.set_ylabel('Credit Count', color=color)
ax0.plot(credit_df['date'], credit_df['credit'], color=color)
ax0.tick_params(axis='y', labelcolor=color)
ax0.title.set_text('Event' + str(eid))

ax1 = ax0.twinx()  # instantiate a second axes that shares the same x-axis

color = 'tab:blue'
ax1.set_ylabel('Tweeter Count', color=color)  # we already handled the x-label with ax1
ax1.plot(count_df['date'], count_df['count'], color=color)
ax1.tick_params(axis='y', labelcolor=color)

color = 'tab:red'
# ax2.set_xlabel('Dates')
ax2.set_ylabel('Event-match Evolving', color=color)
ax2.plot(credit_df_evl_loc['date'], credit_df_evl_loc['sum_credit'], color=color)
ax2.tick_params(axis='y', labelcolor=color)

ax3 = ax2.twinx()  # instantiate a second axes that shares the same x-axis

color = 'tab:blue'
ax3.set_ylabel('Re-tweet Evolving', color=color)  # we already handled the x-label with ax1
ax3.plot(credit_df_evl_rt['date'], credit_df_evl_rt['sum_credit'], color=color)
ax3.tick_params(axis='y', labelcolor=color)

color = 'tab:green'
ax4.set_xlabel('Dates')
ax4.set_ylabel('Merged Evolving', color=color)
ax4.plot(credit_df_evl['date'], credit_df_evl['sum_credit'], color=color)
ax4.tick_params(axis='y', labelcolor=color)

''' Plot '''
fig.tight_layout()  # otherwise the right y-label is slightly clipped
plt.setp(ax0.get_xticklabels(), visible=False)
plt.setp(ax2.get_xticklabels(), visible=False)
plt.show()
