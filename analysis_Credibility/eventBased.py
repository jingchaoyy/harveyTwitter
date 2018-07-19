"""
Created on 7/5/2018
@author: Jingchao Yang
"""
from psqlOperations import queryFromDB
from toolBox import location_tools, fuzzy_gazatteer
from toolBox import events_from_tweets
import jellyfish


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
        gzs = events_from_tweets.remove(gzs)
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
    Geocode all place events and see if it matches any road events
    1. coordinate match
    2. road name match
    3. jellyfish fuzzy road name match with zip code guard
    if matches, merge -> combine tids and remove duplicates, the number of different tids will be the final credits for
    that event (since same location from same resource under same tid should be only count once), and the place name
    will also be added/ merged If no matches, add place events directly after format

    :param eventList: all events
    :return: merged events if applicable, and non mergeable events
    """
    roadEvent, placeEvent = [], []
    for event in eventList:  # separate events to road events and place events based on different tagged symbols
        if event[0].startswith('*R '):
            roadLocate = location_tools.roadToCoor(event[0][3:])
            roadZip = roadLocate[0]
            coor = roadLocate[1]  # assign coordinate
            # print((event[0], [], coor[0], coor[1], event[1], event[2]))
            roadEvent.append((event[0], [], coor[0], coor[1], roadZip, event[1], event[2]))
        if event[0].startswith('#P '):
            placeLocate = location_tools.placeToRoad(event[0][3:])
            roadName = placeLocate[0]
            placeZip = placeLocate[1]
            placeCoor = placeLocate[2]
            placeEvent.append((roadName, event[0], placeCoor[0], placeCoor[1], placeZip, event[1], event[2]))

    for road in roadEvent:
        roadFormat1 = fuzzy_gazatteer.roadNameFormat(str(road[0][3:]))
        scoreList, scoredPlace = [], []  # storing all compared scores, and places used for compare
        for place in placeEvent:
            if place is not None:
                roadFormat2 = fuzzy_gazatteer.roadNameFormat(str(place[0]))

                # '''Using direct coordinate/ road name match'''
                if (place[2], place[3]) == (road[2], road[3]) or roadFormat1 == roadFormat2:
                    tids = road[-1] + place[-1]
                    tids = events_from_tweets.remove(tids)
                    update = (road[0], road[1] + [place[1]], place[2], place[3], place[4], len(tids), tids)
                    placeEvent[placeEvent.index(place)] = None  # set to None to avoid duplicate
                    roadEvent[roadEvent.index(road)] = update  # update road event in roadEvent list
                    road = update
                # '''Using road name fuzzy string match with zip code'''
                else:
                    # giving string match score after formatted
                    score = jellyfish.jaro_distance(roadFormat1, roadFormat2)
                    scoreList.append(score)
                    scoredPlace.append(place)
        if len(scoreList) > 0:
            maxScore = max(scoreList)
            if maxScore > 0.75:  # if 75% matches
                placeInd = scoreList.index(maxScore)
                place = scoredPlace[placeInd]
                if road[4] == place[4]:  # if also zip code matches
                    tids = road[-1] + place[-1]
                    tids = events_from_tweets.remove(tids)
                    update = (road[0], road[1] + [place[1]], road[2], road[3], road[4], len(tids), tids)
                    placeEvent[placeEvent.index(place)] = None  # delete after merged to avoid duplicate
                    roadEvent[roadEvent.index(road)] = update  # update road event in roadEvent list

    rfPlaces = []
    for place in placeEvent:  # reformat the rest of places
        if place is not None:
            rf = ('', [place[1]], place[2], place[3], place[4], place[5], place[6])
            rfPlaces.append(rf)

    finalEvent = roadEvent + rfPlaces
    mergeEvent, coors = [], []  # merge when having same coordinates (above only tested if road event has same coor
    # with place event, this one will test if road and road, place and place events have same coor)
    for final in finalEvent:
        if (final[2], final[3]) in coors:
            ind = coors.index((final[2], final[3]))
            tids = mergeEvent[ind][-1] + final[-1]
            tids = events_from_tweets.remove(tids)
            update = (mergeEvent[ind][0] + final[0], mergeEvent[ind][1] + final[1], mergeEvent[ind][2],
                      mergeEvent[ind][3], mergeEvent[ind][4], len(tids), tids)
            mergeEvent[ind] = update
        else:
            mergeEvent.append(final)
            coors.append((final[2], final[3]))

    return mergeEvent


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
