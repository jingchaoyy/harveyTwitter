"""
Created on  2/7/2019
@author: Jingchao Yang
"""

import psycopg2.extras
from postSaTScan import postSaTScan

tb_in_Name = 'original_credibility_power4'

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
                                               "neighborhood int,"
                                               "neighbors Text,"
                                               "sup_tws int,"
                                               "tIDs Text"
                                               ");")
    conn.commit()
    print("create table succeeded " + tb_in_Name)
except:
    print("create table failed " + tb_in_Name)

sql = "insert into " + tb_in_Name + " values (%s, %s, %s, %s, %s)"
cluster_tids = postSaTScan.cluster_list2
cluster_gazetteers = postSaTScan.gazetteer_list

for i in range(len(cluster_tids)):
    data = (
    i, cluster_tids[i][0], ', '.join(cluster_gazetteers[i][1]), len(cluster_tids[i][1]), ', '.join(cluster_tids[i][0]))
    try:
        cur.execute(sql, data)
        conn.commit()
    except:
        print("I can't insert into " + tb_in_Name)

conn.close()
