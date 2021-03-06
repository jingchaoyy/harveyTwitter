"""
Created on 7/5/2018
@author: Jingchao Yang

Goal: find local gazetteers from text using pattern analysis (tweets or url pages),
then assign scores based on ground truth (jellyfish fuzzy match)
"""
# import pandas as pd
from psqlOperations import queryFromDB
from toolBox import fuzzy_gazatteer, url_tools

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
# table storing event associated tid under same theme
tb1_out_Name = "original_events_power2"
# table storing text from tw
tb2_out_Name = "original"
tb2_clo_text = "ttext"
# table storing text from url
tb3_out_Name = "original_urltext"
tb3_clo_text = "url_text"
# join key
match_clo = "tid"

'''select harvey relief events and shelter events from table original_events with twitter text from table original'''
matchedEvents = queryFromDB.attQueryWJoin2(dbConnect, tb1_out_Name, tb2_out_Name, "tw_key", match_clo,
                                           tb2_clo_text, match_clo)
roads_from_tw, places_from_tw = fuzzy_gazatteer.localGazetter(matchedEvents)
print('fuzzy gazetteers from tweets finished', len(places_from_tw))

'''select harvey relief events and shelter events from table original_events with url text from table test_urltext'''
matchedEvents_url = queryFromDB.attQueryWJoin2(dbConnect, tb1_out_Name, tb3_out_Name, "url_key", match_clo,
                                               tb3_clo_text, match_clo)

filtered = []
for i in matchedEvents_url:
    if i[0] is not None:
        filter = url_tools.paragraphPicker(i)
        filtered.append(filter)

roads_from_url, places_from_url = fuzzy_gazatteer.localGazetter(filtered)
print('fuzzy gazetteers from tweets finished', len(roads_from_url))

# '''Collecting shelter data from ground truth external source'''
# allData = pd.read_csv('C:\\Users\\no281\\Documents\\harVeyTwitter\\groundTruthFromExternal\\harveyShelter.csv')
# roads_from_tru = allData['Address'].values.tolist()
# places_from_tru = allData['Address Name'].values.tolist()
# print('true gazetteers from trust source finished')
