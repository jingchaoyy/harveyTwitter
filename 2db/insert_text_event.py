"""
Created on 6/19/2018
@author: Jingchao Yang
"""
import psycopg2.extras
from psqlOperations import queryFromDB


def collectTID(duList):
    """

    :param duList:
    :return:
    """
    idList = []
    for i in duList:
        if i[0] not in idList:
            idList.append(i[0])

    return idList


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
            keyList[ind] = (i[0], keyList[ind][-1] + ',' + i[-1])
    return keyList


dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
events = ['black out', 'blackout', 'coned', 'dark', 'downed electrical wires', 'POWER down',
          'POWER not expected', 'POWER off', 'POWER out', 'goodbye POWER', 'knock out POWER',
          'lose POWER', 'losing POWER', 'lost POWER', 'no POWER', 'noPOWER', 'off the grid', 'powerless',
          'shut off POWER', 'taken POWER', 'transformer exploding', 'transformer explosion', 'w/o POWER',
          'wait POWER return', 'without pow']
tb_in_Name = 'original_events_power2'
tb_out_Name1 = "original_texteng"
col_Text1 = "eng_text"
tb_out_Name2 = "original_urltext"
col_Text2 = "url_text"

tw_Events = queryFromDB.likeQuery(dbConnect, tb_out_Name1, col_Text1, events)
print('event extraction from tws finished', len(tw_Events))

url_Events = queryFromDB.likeQuery(dbConnect, tb_out_Name2, col_Text2, events)
print('event extraction from tws finished', len(url_Events))

eList = tw_Events + url_Events
eList_tw = remove(tw_Events)
eList_url = remove(url_Events)
keys = [eList_tw, eList_url]

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
                                               "tw_key text,"
                                               "url_key text,"
                                               "tID bigint"
                                               ");")
    conn.commit()
    print("create table succeeded " + tb_in_Name)
except:
    print("create table failed " + tb_in_Name)

''' insert all tid into the table first, can be used as key for later table update '''
all_tid = collectTID(eList)
for tid in range(len(all_tid)):
    try:
        cur.execute("insert into " + tb_in_Name + " (eid, tid) values (" + str(tid) + "," + str(all_tid[tid]) + ")")
        conn.commit()
    except:
        print("I can't insert tid into " + tb_in_Name)

''' update table with keys based on tid '''
key_colNames = ['tw_key', 'url_key']
for key in keys:
    for k in key:
        print("update " + tb_in_Name + " set " + key_colNames[
            keys.index(key)] + " = '" + k[-1] + "' where tid = " + str(k[0]))
        try:
            cur.execute("update " + tb_in_Name + " set " + key_colNames[
                keys.index(key)] + " = '" + k[-1] + "' where tid = " + str(k[0]))
            conn.commit()
        except:
            print("I can't insert gz into " + tb_in_Name)

conn.close()
