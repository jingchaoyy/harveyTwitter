"""
Created on 2/8/2018
@author: Jingchao Yang

Modified version from /analysis_Credibility for SaTScan data
Update original_credibility_power4 table with credibility score from eventBased_selfEvaluate.py
"""
from postSaTScan import eventBased_selfEvaluate
import psycopg2.extras

tb_in_Name = 'original_credibility_power5'

try:
    conn = psycopg2.connect("dbname='harveyTwitts' user='postgres' host='localhost' password='123456'")
except:
    print("I am unable to connect to the database")

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

try:
    cur.execute(
        "alter table " + tb_in_Name + " drop loc_Credits, drop loc_Credit, drop rt_Credits, drop rt_Credit, drop time;")
    conn.commit()
    print("drop columns succeeded " + tb_in_Name)
except:
    print("drop columns failed " + tb_in_Name)
    conn.rollback()  # when command fail, the transaction will be aborted and no further command will be executed
    # until a call to the rollback(). This except will prevent such abort when table is new and cannot be found and drop

try:
    cur.execute(
        "alter table " + tb_in_Name + " add loc_Credit double precision, add rt_Credit int,"
                                      " add loc_Credits double precision[], add rt_Credits int[],"
                                      " add time text;")
    conn.commit()
    print("add columns succeeded " + tb_in_Name)
except:
    print("add columns failed " + tb_in_Name)

eList = eventBased_selfEvaluate.creditList
for i in range(len(eList)):
    sql = "update " + tb_in_Name + " set loc_Credits = array" + str(eList[i][1]) + ", loc_Credit = " + str(
        eList[i][2]) + ", rt_Credits = array" + str(eList[i][3]) + ", rt_Credit = " + str(
        eList[i][4]) + ", time = '" + ','.join(eList[i][-1]) + "' where eid = " + str(eList[i][0])
    try:
        cur.execute(sql)
        conn.commit()
    except:
        print("I can't update " + tb_in_Name)

conn.close()
