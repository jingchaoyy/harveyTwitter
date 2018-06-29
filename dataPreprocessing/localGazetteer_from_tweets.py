"""
Created on 6/28/2018
@author: Jingchao Yang

Goal: Comparing with location (city name) extracted from text, this experiment using local gazetteer information,
providing more more local, and more specific analysis
"""
from psqlOperations import queryFromDB
from toolBox import fuzzy_gazatteer
import pandas as pd

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tb1_out_Name = "original_events"
tb2_out_Name = "original"
tb1_clo_event = "events"
tb2_clo_text = "ttext"
tb1_clo_tid = "tid"
tb2_clo_tid = "tid"
sqlVar1 = 'harvey relief'
sqlVar2 = 'shelter'
matchedEvents = queryFromDB.attQueryWJoin(dbConnect, tb1_out_Name, tb2_out_Name, tb1_clo_event, tb1_clo_tid,
                                          tb2_clo_text, tb2_clo_tid, sqlVar1, sqlVar2)

roads, places = fuzzy_gazatteer.localGazetter(matchedEvents)

