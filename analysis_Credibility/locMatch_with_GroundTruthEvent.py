"""
Created on 6/29/2018
@author: Jingchao Yang

Goal: Comparing local gazetteer information extracted from tws with direct from ground truth sources
"""
import jellyfish
import pandas as pd
from psqlOperations import queryFromDB
from toolBox import fuzzy_gazatteer
import re


def roadNameFormat(roadName):
    """
    Format road name to enhance string match when using jellyfish

    :param roadName: string, original road name
    :return: formatted road name
    """
    searchList = ['north', 'south', 'west', 'east', 'road', 'street', 'drive', 'square', 'boulevard', 'highway',
                  'avenue']  # list for search and to be replaced
    formatList = ['n', 's', 'w', 'e', 'rd', 'st', 'dr', 'sq', 'blvd', 'hwy', 'ave']

    # format road names
    loc11Split = str(roadName).split(' ')
    loc11Format = []
    for j in loc11Split:
        if j.lower() in searchList:
            formated = formatList[searchList.index(j.lower())]
            loc11Format.append(formated)
        else:
            j = j.lower()
            j = re.sub(r'[^\w]', ' ', j)
            j = j.strip()
            loc11Format.append(j)
    roadName = ' '.join(loc11Format)
    return roadName


def fuzzyLocMatch(locList1, locList2):
    """
    Fuzzy location match using string comparision with jellyfish, can be applied to tweets extracted local gazetteers and ground truth
    or url extracted local gazetteers and ground truth

    :param locList1: tw extracted local gazetteers
    :param locList2: ground truth
    :return: score with tid (how reliable the tw is based only on either address or place name fuzzy match)
    """
    scores = []
    for loc1 in locList1:
        print(loc1[-1])
        score = 0
        for loc11 in loc1[0]:
            loc11 = roadNameFormat(loc11)

            for loc2 in locList2:
                loc2 = roadNameFormat(loc2)

                s = jellyfish.jaro_distance(str(loc11), str(loc2))
                # print(str(loc11), '###', str(loc2), s)
                score = max(score, s)

        scores.append((score, loc1[-1]))
    return scores


dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tb1_out_Name = "test_events"
tb2_out_Name = "test"
tb1_clo_event = "events"
tb2_clo_text = "ttext"
tb1_clo_tid = "tid"
tb2_clo_tid = "tid"
sqlVar1 = 'harvey relief'
sqlVar2 = 'shelter'

'''select harvey relief events and shelter events from table original_events with twitter text from table original'''
matchedEvents = queryFromDB.attQueryWJoin(dbConnect, tb1_out_Name, tb2_out_Name, tb1_clo_event, tb1_clo_tid,
                                          tb2_clo_text, tb2_clo_tid, sqlVar1, sqlVar2)
roads_from_tw, places_from_tw = fuzzy_gazatteer.localGazetter(matchedEvents)
print('fuzzy gazetteers from tweets finished', len(places_from_tw))

tb3_out_Name = "test_urltext"
tb3_clo_text = "url_text"

'''select harvey relief events and shelter events from table original_events with url text from table test_urltext'''
matchedEvents_url = queryFromDB.attQueryWJoin(dbConnect, tb1_out_Name, tb3_out_Name, tb1_clo_event, tb1_clo_tid,
                                              tb3_clo_text, tb2_clo_tid, sqlVar1, sqlVar2)
roads_from_url, places_from_url = fuzzy_gazatteer.localGazetter(matchedEvents_url)
print('fuzzy gazetteers from tweets finished', len(roads_from_url))

'''Collecting shelter data from ground truth external source'''
allData = pd.read_csv('C:\\Users\\no281\\Documents\\harVeyTwitter\\groundTruthFromExternal\\harveyShelter.csv')
roads_from_tru = allData['Address'].values.tolist()
places_from_tru = allData['Address Name'].values.tolist()
print('true gazetteers from trust source finished')

'''fuzzyLocMatch'''
roadScores = fuzzyLocMatch(roads_from_url, roads_from_tru)
for roadScore in roadScores:
    print(roadScore)
# placeScores = fuzzyLocMatch(places_from_tw, places_from_tru)
#
# '''matchBoost'''
