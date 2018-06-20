"""
Created on 6/19/2018
@author: Jingchao Yang
"""

import psycopg2.extras
from analysis import events_from_tweets

tb_in_Name = 'original_events'

try:
    conn = psycopg2.connect("dbname='harveyTwitts' user='postgres' host='localhost' password='123456'")
except:
    print("I am unable to connect to the database")

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

eList = events_from_tweets.text_Events

sql = "insert into " + tb_in_Name + " values (%s, %s, %s)"

for i in range(len(eList)):
    data = (i, eList[i][1], eList[i][0])
    try:
        cur.execute(sql, data)
        conn.commit()
    except:
        print("I can't insert into" + tb_in_Name)

conn.close()
