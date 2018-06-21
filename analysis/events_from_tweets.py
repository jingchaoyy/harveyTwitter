"""
Created on 6/19/2018
@author: Jingchao Yang
"""


def eventBack(twList, eList):
    # twList: original tweet list
    # eList: list of events
    twWithEvents = []
    for tw in twList:
        for event in eList:
            if event in tw[1].lower():
                twWithEvents.append((tw[0], event))
    return twWithEvents
