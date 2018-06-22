"""
Created on 6/19/2018
@author: Jingchao Yang
"""
import psycopg2.extras
from dataPreprocessing import events_from_tweets
from psqlOperations import queryClean

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tb_in_Name = 'original_events'
tb_out_Name = "original"
clo_Text = "ttext"
data_text = queryClean.singleColumn_wFilter(dbConnect, tb_out_Name, clo_Text)
print('Original English Only Tweets', len(data_text))

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
