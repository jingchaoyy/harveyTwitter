"""
Created on 6/19/2018
@author: Jingchao Yang
"""
import psycopg2.extras
from psqlOperations import queryFromDB


def remove(duList):
    """

    :param duList:
    :return:
    """
    idList, keyList = [], []
    for i in duList:
        if i[0] not in idList:
            idList.append(i[0])
            keyList.append(i)
        else:
            ind = idList.index(i[0])
            keyList[ind] = (i[0], keyList[ind][-1]+',' + i[-1])
    return keyList


dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
events = ['hotel', 'housing', 'shelter']
tb_in_Name = 'original_events'
tb_out_Name1 = "original_texteng"
col_Text1 = "eng_text"
tb_out_Name2 = "original_urltext"
col_Text2 = "url_text"

tw_Events = queryFromDB.likeQuery(dbConnect, tb_out_Name1, col_Text1, events)
print('event extraction from tws finished', len(tw_Events))

url_Events = queryFromDB.likeQuery(dbConnect, tb_out_Name2, col_Text2, events)
print('event extraction from tws finished', len(url_Events))

eList = tw_Events + url_Events
eList = remove(eList)

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
                                               "eventkey text,"
                                               "tID bigint"
                                               ");")
    conn.commit()
    print("create table succeeded " + tb_in_Name)
except:
    print("create table failed " + tb_in_Name)

sql = "insert into " + tb_in_Name + " values (%s, %s, %s)"
for i in range(len(eList)):
    data = (i, eList[i][-1], eList[i][0])
    try:
        cur.execute(sql, data)
        conn.commit()
    except:
        print("I can't insert into " + tb_in_Name)

conn.close()
