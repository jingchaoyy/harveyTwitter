"""
Created on 6/19/2018
@author: Jingchao Yang
"""

import psycopg2.extras
from psqlOperations import queryFromDB

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
ta_out_Name = "test"
tb_in_Name = 'test_UserCoor'

clo_Lat = "tlat"
clo_Lon = "tlon"
user_Coors = queryFromDB.get_coorData(dbConnect, ta_out_Name, clo_Lat, clo_Lon)
print(user_Coors)

try:
    conn = psycopg2.connect("dbname='harveyTwitts' user='postgres' host='localhost' password='123456'")
except:
    print("I am unable to connect to the database")

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

tLatlonList = user_Coors

sql = "insert into " + tb_in_Name + " values (%s, %s, %s, %s)"

for i in range(len(tLatlonList)):
    data = (i, tLatlonList[i][1], tLatlonList[i][2], tLatlonList[i][0])
    try:
        cur.execute(sql, data)
        conn.commit()
    except:
        print("I can't insert into Test")

conn.close()
