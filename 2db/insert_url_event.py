"""
Created on 6/21/2018
@author: no281
"""
import psycopg2.extras
from toolBox import url_tools
from psqlOperations import queryFromDB
from analysis import events_from_tweets

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tb_in_Name = 'original_urlevents'
tb_out_Name = "original"
clo_url = "teu_url"
allURLs = queryFromDB.get_colData(dbConnect, tb_out_Name, clo_url)
print('url collecting finished', len(allURLs))

filters = ['twitter.com', 'youtube.com', 'instagram.com']  # remove links that are from social media
filteredURLs = url_tools.urlFilter(allURLs, filters)
print('url filtering finished', len(filteredURLs))

data_text = events_from_tweets.textExtractor(filteredURLs)
print('extract text from url finished', len(data_text))

events = ['infection', 'toxic', 'rescue', 'power', 'mosquitoes', 'harvey relief', 'donate']
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
