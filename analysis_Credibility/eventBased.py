"""
Created on 7/5/2018
@author: Jingchao Yang
"""
from psqlOperations import queryFromDB
from toolBox import location_tools, fuzzy_gazatteer
import jellyfish


def remove(list):
    """
    Remove duplicate location name. Same location under same resource with same tid should give only one credit,
    instead of multiple

    :param list: location list
    :return: nun-duplicate location list
    """
    removed = []
    for i in list:
        if i not in removed:
            removed.append(i)
    return removed


def extract(gzList):
    """
    Count location from all tweets, each count means a credit to the event
    Event means theme + location + time (will be analysis in later processes), for now, since all location were
    collected under same theme, and time is not currently involved into consideration, each different location
    will represent as different event.

    :param gzList: gazetteer list
    :return: event list, event associated tid list, and event associated credit list
    """
    eventList, tidList, creList = [], [], []
    for gzset in gzList:
        gzs = gzset[0].split(',')
        gzs = remove(gzs)
        for gz in gzs:
            gz = gz.strip()
            if gz != '':
                if gz not in eventList:  # when new event shows
                    tidList_tem = []  # new tem tidList
                    eventList.append(gz)
                    creList.append(1)  # initial the credit as 1
                    tidList_tem.append(gzset[-1])
                    tidList.append(tidList_tem)  # new append to tidList
                else:  # if event exist
                    creList[eventList.index(gz)] = creList[eventList.index(gz)] + 1  # credit increase by 1 at a time
                    tidList[eventList.index(gz)].append(gzset[-1])  # append to associated tid list

    return eventList, tidList, creList


def extractEvent(gzList1, gzList2):
    """
    Extract event based gazetteer (when a location appeared more than once in the same
    source (tw or url) under same tid, count only once)

    :param gzList1: gazetteer list from tweets
    :param gzList2: gazetteer list from urls
    :return: merged possible event list, event associated tid list, and event associated credit list
    """
    tw_event, tw_tid, tw_cre = extract(gzList1)  # extract event separately, need to remove duplicate by resource
    url_event, url_tid, url_cre = extract(gzList2)

    # merged separate-extracted event list with their associated tids and credit
    eventList, tidList, creList = [], [], []
    for twe in tw_event:
        if twe in url_event:  # when same event found from both resources,
            # merge by accumulating credit score and append tid list
            eventList.append(twe)
            cre = tw_cre[tw_event.index(twe)] + url_cre[url_event.index(twe)]
            creList.append(cre)
            tids = tw_tid[tw_event.index(twe)] + url_tid[url_event.index(twe)]
            tidList.append(tids)
        else:  # otherwise, append directly
            eventList.append(twe)
            creList.append(tw_cre[tw_event.index(twe)])
            tidList.append(tw_tid[tw_event.index(twe)])
    for urle in url_event:  # append directly
        if urle not in eventList:
            eventList.append(urle)
            creList.append(url_cre[url_event.index(urle)])
            tidList.append(url_tid[url_event.index(urle)])
    return eventList, tidList, creList


def eventFinalize(eventList):
    """
    Geocode all place events and see if it matches any road events (using jellyfish after format road names from event
    list and from geocoding), if it does, merge -> combine tids and remove duplicates, the number of different tids
    will be the final credits for that event (since same location from same resource under same tid should be only count
    once), and the place name will also be added/ merged
    If no matches, add place events directly after format

    :param eventList: all events
    :return: merged events if applicable, and non mergeable events
    """
    roadEvent, placeEvent = [], []
    for event in eventList:  # separate events to road events and place events based on different tagged symbols
        if event[0].startswith('*R '):
            coor = location_tools.roadToCoor(event[0][3:])  # assign coordinate
            roadEvent.append((event[0], [], coor[0], coor[1], event[1], event[2]))
        elif event[0].startswith('#P '):
            geoLocate = location_tools.placeToRoad(event[0][3:])
            roadName = geoLocate[0]
            placeCoor = geoLocate[1]
            placeEvent.append((roadName, event[0], placeCoor[0], placeCoor[1], event[1], event[2]))

    for place in placeEvent:
        scoreList = []  # storing all compared scores
        for road in roadEvent:
            # giving string match score after formatted
            score = jellyfish.jaro_distance(fuzzy_gazatteer.roadNameFormat(str(road[0][3:])),
                                            fuzzy_gazatteer.roadNameFormat(str(place[0])))
            scoreList.append(score)

        maxScore = max(scoreList)
        if maxScore > 0.75:  # if 75% matches
            roadInd = scoreList.index(maxScore)
            road = roadEvent[roadInd]
            tids = road[-1] + place[-1]
            tids = remove(tids)
            update = (road[0], road[1] + [place[1]], road[2], road[3], len(tids), tids)
            placeEvent.remove(place)  # delete after merged to avoid duplicate

            roadEvent.remove(road)  # update road event in roadEvent list
            roadEvent.append(update)

    rfPlaces = []
    for place in placeEvent:  # reformat the rest of places
        rf = ('', [place[1]], place[2], place[3], place[4], place[5])
        rfPlaces.append(rf)

    finalEvent = roadEvent + rfPlaces
    return finalEvent


'''databsed connection variables'''
dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
gz_tb = "original_gazetteer"
col1 = "tw_road"
col2 = "tw_place"
col3 = "url_road"
col4 = "url_place"

tw_gz = queryFromDB.mergeSelect(dbConnect, gz_tb, col1, col2)
url_gz = queryFromDB.mergeSelect(dbConnect, gz_tb, col3, col4)
events, tids, credits = extractEvent(tw_gz, url_gz)
allEvents = zip(events, credits, tids)
finalized = eventFinalize(allEvents)
print('event finalize finished', len(finalized))
# for f in finalized:
#     print(f)
