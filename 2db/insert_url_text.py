"""
Created on 6/25/2018
@author: Jingchao Yang
"""
import psycopg2.extras
from toolBox import url_tools, events_from_tweets
from psqlOperations import queryFromDB

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tb_in_Name = 'original_urltext'
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

try:
    conn = psycopg2.connect("dbname='harveyTwitts' user='postgres' host='localhost' password='123456'")
except:
    print("I am unable to connect to the database")

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

eList = data_text

sql = "insert into " + tb_in_Name + " values (%s, %s, %s)"

for i in range(len(eList)):
    data = (i, eList[i][1], eList[i][0])
    try:
        cur.execute(sql, data)
        conn.commit()
    except:
        print("I can't insert into" + tb_in_Name)

conn.close()