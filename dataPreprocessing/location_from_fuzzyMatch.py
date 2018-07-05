"""
Created on 7/5/2018
@author: Jingchao Yang
"""
import pandas as pd
from psqlOperations import queryFromDB
from toolBox import fuzzy_gazatteer

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tb1_out_Name = "test_events"
tb2_out_Name = "test"
tb1_clo_event = "events"
tb2_clo_text = "ttext"
match_clo = "tid"
sqlVar1 = 'harvey relief'
sqlVar2 = 'shelter'

'''select harvey relief events and shelter events from table original_events with twitter text from table original'''
matchedEvents = queryFromDB.attQueryWJoin(dbConnect, tb1_out_Name, tb2_out_Name, tb1_clo_event, match_clo,
                                          tb2_clo_text, match_clo, sqlVar1, sqlVar2)
roads_from_tw, places_from_tw = fuzzy_gazatteer.localGazetter(matchedEvents)
print('fuzzy gazetteers from tweets finished', len(places_from_tw))

tb3_out_Name = "test_urltext"
tb3_clo_text = "url_text"

'''select harvey relief events and shelter events from table original_events with url text from table test_urltext'''
matchedEvents_url = queryFromDB.attQueryWJoin(dbConnect, tb1_out_Name, tb3_out_Name, tb1_clo_event, match_clo,
                                              tb3_clo_text, match_clo, sqlVar1, sqlVar2)
roads_from_url, places_from_url = fuzzy_gazatteer.localGazetter(matchedEvents_url)
print('fuzzy gazetteers from tweets finished', len(roads_from_url))

'''Collecting shelter data from ground truth external source'''
allData = pd.read_csv('C:\\Users\\no281\\Documents\\harVeyTwitter\\groundTruthFromExternal\\harveyShelter.csv')
roads_from_tru = allData['Address'].values.tolist()
places_from_tru = allData['Address Name'].values.tolist()
print('true gazetteers from trust source finished')

'''fuzzyLocMatch'''
roadScores = fuzzy_gazatteer.fuzzyLocMatch(roads_from_url, roads_from_tru)
for roadScore in roadScores:
    print(roadScore)
