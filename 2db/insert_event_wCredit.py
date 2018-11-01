"""
Created on 7/16/2018
@author: Jingchao Yang

insert all events with location, and associated credibility
"""
from analysis_Credibility import eventBased
import psycopg2.extras

tb_in_Name = 'original_credibility_power3'

try:
    conn = psycopg2.connect("dbname='harveyTwitts' user='postgres' host='localhost' password='123456'")
except:
    print("I am unable to connect to the database")

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

try:
    cur.execute("drop table " + tb_in_Name)
    conn.commit()
    print("drop table succeeded " + tb_in_Name)
except:
    print("drop table failed " + tb_in_Name)
    conn.rollback()  # when command fail, the transaction will be aborted and no further command will be executed
    # until a call to the rollback(). This except will prevent such abort when table is new and cannot be found and drop

try:
    cur.execute("create table " + tb_in_Name + "("
                                               "eID int PRIMARY KEY NOT NULL,"
                                               "road_events Text,"
                                               "place_events Text,"
                                               "Lat double precision,"
                                               "Lng double precision,"
                                               "zip_code Text,"
                                               "sup_tws int,"
                                               "tIDs Text"
                                               ");")
    conn.commit()
    print("create table succeeded " + tb_in_Name)
except:
    print("create table failed " + tb_in_Name)

sql = "insert into " + tb_in_Name + " values (%s, %s, %s, %s, %s, %s, %s, %s)"
eList = eventBased.finalized
for i in range(len(eList)):
    data = (i, ', '.join(eList[i][0]), ', '.join(eList[i][1]), eList[i][2], eList[i][3], eList[i][4], eList[i][5],
            ', '.join(str(e) for e in eList[i][6]))
    try:
        cur.execute(sql, data)
        conn.commit()
    except:
        print("I can't insert into " + tb_in_Name)

conn.close()
