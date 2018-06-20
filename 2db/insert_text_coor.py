"""
Created on 6/19/2018
@author: Jingchao Yang
"""

import psycopg2.extras
from psqlOperations import queryClean
from analysis import location_tools

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tb_out_Name = "original"
tb_in_Name = 'original_textCoor'

############ Location from text
clo_Text = "ttext"
data_text = queryClean.singleColumn_wFilter(dbConnect, tb_out_Name, clo_Text)
# print('Original English Only Tweets', data_text)

setCountry = 'United States'
locFilter = ['Harvey', 'Hurricane']  # Name list that should not be considered as location under certain event
loc_fromText = location_tools.locFromText(setCountry, data_text, locFilter)
# print('All locations extracted', loc_fromText)

loc_nonDup = location_tools.Remove(loc_fromText)
# print('Non duplicate location list', loc_nonDup)

coorFromLoc_nonDup = location_tools.locToCoor(loc_nonDup)
# print('Associated coordinates', coorFromLoc_nonDup)

text_LocCoors = location_tools.coorToTweets(coorFromLoc_nonDup, loc_fromText)
# print(text_LocCoors)


try:
    conn = psycopg2.connect("dbname='harveyTwitts' user='postgres' host='localhost' password='123456'")
except:
    print("I am unable to connect to the database")

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

textLatlonList = text_LocCoors

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
