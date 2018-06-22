"""
Created on 6/20/2018
@author: Jingchao Yang
"""
import psycopg2.extras
from dataPreprocessing import location_from_urls

tb_in_Name = 'original_urlcoor'

try:
    conn = psycopg2.connect("dbname='harveyTwitts' user='postgres' host='localhost' password='123456'")
except:
    print("I am unable to connect to the database")

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

textLatlonList = location_from_urls.text_LocCoors
print("Start insert into", tb_in_Name)

sql = "insert into " + tb_in_Name + " values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

for i in range(len(textLatlonList)):
    data = (i, textLatlonList[i][1], textLatlonList[i][2][0], textLatlonList[i][2][1], textLatlonList[i][2][2],
            textLatlonList[i][2][3], textLatlonList[i][2][4], textLatlonList[i][2][5], textLatlonList[i][0])
    try:
        cur.execute(sql, data)
        conn.commit()
    except:
        print("I can't insert into" + tb_in_Name)

conn.close()
