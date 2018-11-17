"""
Created on 11/7/2018
@author: Jingchao Yang
"""
from analysis_Credibility import spatialAggregation
import psycopg2.extras

tb_in_Name = 'original_credibility_power3_locMerge'

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
                                               "locMerge_ID int,"
                                               "original_ID text,"
                                               "time text,"
                                               "loc_Credit double precision,"
                                               "rt_Credit double precision,"
                                               "loc_Credits double precision[],"
                                               "rt_Credits double precision[]"
                                               ");")
    conn.commit()
    print("create table succeeded " + tb_in_Name)
except:
    print("create table failed " + tb_in_Name)

sql = "insert into " + tb_in_Name + " values (%s, %s, %s, %s,%s, %s, %s, %s)"
eList = spatialAggregation.resultSet
maxRT = spatialAggregation.maxRT
for i in range(len(eList)):
    locMerge_ID = eList[i].locMergeID
    original_ID = ','.join([str(j) for j in eList[i].orgID])
    time = ','.join(eList[i].time)
    loc_Credits = eList[i].loc_credits / maxRT
    rt_Credits = eList[i].rt_credits / maxRT
    loc_Credit = sum(loc_Credits) / maxRT
    rt_Credit = sum(rt_Credits) / maxRT

    data = (i, locMerge_ID, original_ID, time, loc_Credit, rt_Credit, loc_Credits, rt_Credits)
    try:
        cur.execute(sql, data)
        conn.commit()
    except:
        print("I can't insert into " + tb_in_Name)

conn.close()
