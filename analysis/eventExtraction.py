"""
Created on 6/19/2018
@author: Jingchao Yang
"""
from psqlOperations import queryClean


def eventBack(twList, eList):
    # twList: original tweet list
    # eList: list of events
    twWithEvents = []
    for tw in twList:
        for event in eList:
            if event in tw[1].lower():
                twWithEvents.append((tw[0], event))
    return twWithEvents


dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tabName = "test"
clo_Text = "ttext"
data_text = queryClean.singleColumn_wFilter(dbConnect, tabName, clo_Text)
print('Original English Only Tweets', data_text)

events = ['infection', 'toxic', 'rescue', 'power', 'mosquitoes', 'harvey relief']
text_Events = eventBack(data_text, events)
print(text_Events)
