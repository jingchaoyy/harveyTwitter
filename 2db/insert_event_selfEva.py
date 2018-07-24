"""
Created on 7/24/2018
@author: Jingchao Yang
"""
"""
Created on 7/16/2018
@author: Jingchao Yang

insert all events with location, and associated credibility
"""
from analysis_Credibility import eventBased_selfEvaluate
import psycopg2.extras

tb_in_Name = 'original_credibility_improved'

try:
    conn = psycopg2.connect("dbname='harveyTwitts' user='postgres' host='localhost' password='123456'")
except:
    print("I am unable to connect to the database")

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

try:
    cur.execute("alter table " + tb_in_Name + " drop loc_Credit, drop rt_Credit;")
    conn.commit()
    print("drop table column succeeded " + tb_in_Name)
except:
    print("drop table failed " + tb_in_Name)
    conn.rollback()  # when command fail, the transaction will be aborted and no further command will be executed
    # until a call to the rollback(). This except will prevent such abort when table is new and cannot be found and drop

try:
    cur.execute("alter table " + tb_in_Name + " add loc_Credit double precision,"
                                              " add rt_Credit double precision"
                                              ";")
    conn.commit()
    print("alter table succeeded " + tb_in_Name)
except:
    print("create table failed " + tb_in_Name)

eList = eventBased_selfEvaluate.creditList
for i in range(len(eList)):
    try:
        cur.execute("update " + tb_in_Name + " set loc_Credit = " + str(eList[i][1])
                    + ", set rt_Credit = " + str(eList[i][2]) + " where eid = " + str(eList[i][0]))
        conn.commit()
    except:
        print("I can't insert into " + tb_in_Name)

conn.close()
