"""
Created on 6/22/2018
@author: Jingchao Yang

Level Two credibility test include basic event based cross check
Event sources includes:
Twitter embedded text events extraction
Twitter linked web page text events extraction

Goal is to analysis if events from a same twitter are correlated (same)
"""
from psqlOperations import queryFromDB


def checkEvents(eventList1, eventList2):
    """if same event found within tweets and url page under same tid"""
    matchByEvent = []
    for e1 in eventList1:
        tid = e1[2]
        print(tid)
        for e2 in eventList2:
            if e1[-2:] == e2[-2:]:
                event = e1[1]
                print(event)
                matchByEvent.append((tid, event))
    return matchByEvent


'''databsed connection variables'''
dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tw_event_tb = "test_events"
url_event_tb = "test_urlevents"

'''data select from db'''
tw_events = queryFromDB.get_allData(dbConnect, tw_event_tb)
print("events from tweets", len(tw_events))
url_events = queryFromDB.get_allData(dbConnect, url_event_tb)
print("events from tweets", len(url_events))

matchEvent = checkEvents(tw_events, url_events)
print(matchEvent)
