"""
Created on 7/24/2018
@author: Jingchao Yang
"""

from psqlOperations import queryFromDB
from toolBox import events_from_tweets

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"


def assignCredit(events):
    """

    :param events:
    :return:
    """
    tb2_out_Name = "original_gazetteer"
    col = "tid"
    locCredits = []
    resourceType = ['tw', 'url']
    for event in events:
        eid = event[0]
        tids = event[-1]
        tids = tids.split(',')
        roadEvents, placeEvents = [], []
        if event[1] is not None:
            roadEvents = events_from_tweets.remove(event[1].split(','))
        if event[2] is not None:
            placeEvents = events_from_tweets.remove(event[2].split(','))
        mergedEvents = roadEvents + placeEvents
        locCredit = 0
        for tid in tids:
            tw_roads, tw_places, url_roads, url_places = [], [], [], []
            gazetteer = queryFromDB.query(dbConnect, tb2_out_Name, col, tid)
            gazetteer = gazetteer[0]
            if gazetteer[1] is not None:
                tw_roads = events_from_tweets.remove(gazetteer[1].split(','))
            if gazetteer[2] is not None:
                tw_places = events_from_tweets.remove(gazetteer[2].split(','))
            if gazetteer[3] is not None:
                url_roads = events_from_tweets.remove(gazetteer[3].split(','))
            if gazetteer[4] is not None:
                url_places = events_from_tweets.remove(gazetteer[4].split(','))
            twEvents = tw_roads + tw_places
            urlEvents = url_roads + url_places
            for twe in twEvents:
                if twe in mergedEvents:
                    locCredit = locCredit + 1
            for urle in urlEvents:
                if urle in mergedEvents:
                    locCredit = locCredit + 1
        print((eid, locCredit / len(resourceType)))
        locCredits.append((eid, locCredit / len(resourceType)))


tb1_out_Name = "original_credibility_improved"
colList1 = ['eid', 'road_events', 'place_events', 'tids']

data_events = queryFromDB.get_multiColData(dbConnect, tb1_out_Name, colList1)
# for i in data_events:
#     print(i)
assignCredit(data_events)
