"""
Created on 11/18/2018
@author: Jingchao Yang
"""
from psqlOperations import queryFromDB
import matplotlib.pyplot as plt
import pandas as pd

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tb_out_Event = "original_credibility_power3_locmerge"
colList = ['time', 'loc_credits', 'rt_credits']
eid = '315'


def groupBy(timeList, valueList, groupby):
    """

    :param timeList: time list
    :param valueList: time correlated value
    :param groupby: groupby date or hour
    :return: accumulated list groupby time
    """
    df = pd.DataFrame({'time': timeList, 'Values': valueList})
    if groupby == 'hour':
        df["'" + groupby + "'"] = pd.to_datetime(df['time']).map(lambda dt: dt.hour)

    group = df.groupby("'" + groupby + "'")['Values'].sum()

    # revert back from Series data to DataFrame
    result = pd.DataFrame({groupby: group.index, 'Values': group.values})

    return result


def accuDF(df):
    """

    :param df:
    :return:
    """
    dfAccu = df.assign(sum_credit=df.Values.cumsum())
    return dfAccu


print('extracting data from', tb_out_Event)
data = queryFromDB.get_multiColData_where(dbConnect, tb_out_Event, colList, eid)

'''get data'''
time = data[0].split(',')
count = [1 for i in range(len(time))]
loc_credits = data[1]
rt_credits = data[2]
print('time', time)
print('count', count)
print('loc_credits', loc_credits)
print('rt_credits', rt_credits)

'''group by'''
byHour = 'hour'
outputCol = 'Values'
countGrp = groupBy(time, count, byHour)
locGrp = groupBy(time, loc_credits, byHour)
rtGrp = groupBy(time, rt_credits, byHour)

'''accumulated'''
outputCol2 = 'sum_credit'
countAccu = accuDF(countGrp)
locAccu = accuDF(locGrp)
rtAccu = accuDF(rtGrp)

'''merged'''
mergedAccu = pd.DataFrame({byHour: locAccu[byHour], outputCol2: locAccu[outputCol2] + rtAccu[outputCol2]})
# print(mergedAccu)

''' Paper Section 4.3.1 '''
fig, (ax0, ax2, ax4) = plt.subplots(3, sharey=True)

color = 'tab:red'
# ax0.set_xlabel('Dates')
ax0.set_ylabel('Credit Count', color=color)
ax0.plot(locGrp[byHour], locGrp[outputCol], color=color)
ax0.tick_params(axis='y', labelcolor=color)
ax0.title.set_text('Event' + str(eid))

ax1 = ax0.twinx()  # instantiate a second axes that shares the same x-axis

color = 'tab:blue'
ax1.set_ylabel('Tweeter Count', color=color)  # we already handled the x-label with ax1
ax1.plot(countGrp[byHour], countGrp[outputCol], color=color)
ax1.tick_params(axis='y', labelcolor=color)

color = 'tab:red'
# ax2.set_xlabel('Dates')
ax2.set_ylabel('Event-match Evolving', color=color)
ax2.plot(locAccu[byHour], locAccu[outputCol2], color=color)
ax2.tick_params(axis='y', labelcolor=color)

ax3 = ax2.twinx()  # instantiate a second axes that shares the same x-axis

color = 'tab:blue'
ax3.set_ylabel('Re-tweet Evolving', color=color)  # we already handled the x-label with ax1
ax3.plot(rtAccu[byHour], rtAccu[outputCol2], color=color)
ax3.tick_params(axis='y', labelcolor=color)

color = 'tab:green'
ax4.set_xlabel('Hour')
ax4.set_ylabel('Merged Evolving', color=color)
ax4.plot(mergedAccu[byHour], mergedAccu[outputCol2], color=color)
ax4.tick_params(axis='y', labelcolor=color)

''' Plot '''
fig.tight_layout()  # otherwise the right y-label is slightly clipped
plt.setp(ax0.get_xticklabels(), visible=False)
plt.setp(ax2.get_xticklabels(), visible=False)
plt.show()
