"""
Created on 6/29/2018
@author: Jingchao Yang

Goal: Comparing local gazetteer information extracted from tws with direct from ground truth sources
"""
import jellyfish
import pandas as pd
from psqlOperations import queryFromDB
from toolBox import fuzzy_gazatteer


def fuzzyLocMatch(locList1, locList2):
    """
    Fuzzy location match using string comparision with jellyfish, can be applied to tweets extracted local gazetteers and ground truth
    or url extracted local gazetteers and ground truth

    :param locList1: tw extracted local gazetteers
    :param locList2: ground truth
    :return: score with tid (how reliable the tw is based only on location fuzzy match
    """
    scores = []
    for loc1 in locList1:
        print(loc1[-1])
        for loc11 in loc1[0]:
            score = 0
            for loc2 in locList2:
                if score != 0:
                    s = jellyfish.jaro_distance(str(loc11), str(loc2))
                    score = max(score, s)
                else:
                    score = jellyfish.jaro_distance(str(loc11), str(loc2))
        scores.append((score, loc1[-1]))
    return scores


dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tb1_out_Name = "original_events"
tb2_out_Name = "original"
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
print('fuzzy gazetteers from tweets finished')

'''Collecting shelter data from ground truth external source'''
allData = pd.read_csv('C:\\Users\\no281\\Documents\\harVeyTwitter\\groundTruthFromExternal\\harveyShelter.csv')
roads_from_tru = allData['Address'].values.tolist()
places_from_tru = allData['Address Name'].values.tolist()
print('true gazetteers from trust source finished')

roadScores = fuzzyLocMatch(roads_from_tw, roads_from_tru)
placeScores = fuzzyLocMatch(places_from_tw, places_from_tru)
