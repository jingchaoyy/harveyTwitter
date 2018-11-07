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
                                               "time text"
                                               ");")
    conn.commit()
    print("create table succeeded " + tb_in_Name)
except:
    print("create table failed " + tb_in_Name)

sql = "insert into " + tb_in_Name + " values (%s, %s, %s, %s)"
eList = spatialAggregation.resultSet
for i in range(len(eList)):
    locMerge_ID = eList[i].locMergeID
    original_ID = ','.join([str(j) for j in eList[i].orgID])
    time = ','.join(eList[i].time)
    data = (i, locMerge_ID, original_ID, time)
    try:
        cur.execute(sql, data)
        conn.commit()
    except:
        print("I can't insert into " + tb_in_Name)

conn.close()
