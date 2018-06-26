"""
Created on 6/21/2018
@author: no281
"""
import psycopg2.extras
from toolBox import url_tools
from psqlOperations import queryFromDB
from dataPreprocessing import events_from_tweets
import math

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tb_in_Name = 'original_urlevents'
tb1_out_Name = "original"
tb2_out_Name = "original_texteng"
tb1_clo_url = "teu_url"
tb1_clo_tid = "tid"
tb2_clo_tid = "tid"
allURLs = queryFromDB.joinQuery(dbConnect, tb1_out_Name, tb2_out_Name, tb1_clo_url, tb1_clo_tid, tb2_clo_tid)
print('url collecting finished', len(allURLs))

filters = ['twitter.com', 'youtube.com', 'instagram.com']  # remove links that are from social media
filteredURLs = url_tools.urlFilter(allURLs, filters)
print('url filtering finished', len(filteredURLs))

data_text = events_from_tweets.textExtractor(filteredURLs)
print('extract text from url finished', len(data_text))

events = ['infect', 'toxic', 'rescu', 'power', 'mosquito', 'harvey relief', 'donat', 'flood', 'suppl', 'aid',
          'volunteer', 'high water', 'highwater', 'shelter', 'boat', 'grocery', 'gas', 'highway', 'hwy', 'death',
          'damage', 'destruction', 'debris', 'casualty', 'caution', 'outage', 'water system', 'devastation', 'leak']

# textListLimit = 10000  # slice the list to smaller pieces to avoid memory issue
# if len(data_text) > textListLimit:
#     print("exceeding max query limit", len(data_text))
#     loops = math.ceil(len(data_text) / textListLimit)
#     text_Events = []
#     for j in range(loops):
#         croopedList = data_text[j * textListLimit:(j + 1) * textListLimit]
#         text_Event = events_from_tweets.eventBack(croopedList, events)
#         text_Events += text_Event
# else:
#     text_Events = events_from_tweets.eventBack(data_text, events)

text_Events = events_from_tweets.eventBack(data_text, events)
print('event extraction finished', len(text_Events))

try:
    conn = psycopg2.connect("dbname='harveyTwitts' user='postgres' host='localhost' password='123456'")
except:
    print("I am unable to connect to the database")

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

eList = text_Events

sql = "insert into " + tb_in_Name + " values (%s, %s, %s)"

for i in range(len(eList)):
    data = (i, eList[i][1], eList[i][0])
    try:
        cur.execute(sql, data)
        conn.commit()
    except:
        print("I can't insert into" + tb_in_Name)

conn.close()
