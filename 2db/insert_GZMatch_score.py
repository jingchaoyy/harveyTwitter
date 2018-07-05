"""
Created on 7/5/2018
@author: Jingchao Yang

Goal: insert local gazetteer match scores to database
"""
import psycopg2.extras
from dataPreprocessing import location_from_fuzzyMatch


def collectTID(list):
    """
    Collect all possible tids

    :param list: set of lists that may contain multiple record for one tid
    :return: list of non duplicated tid list, will be used as indicators when updating db table
    """
    tidList = []
    for i in list:
        for j in i:
            if j[-1] not in tidList:
                tidList.append(j[-1])
    return tidList


dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tb_in_Name = 'test_matchscores'

try:
    conn = psycopg2.connect("dbname='harveyTwitts' user='postgres' host='localhost' password='123456'")
except:
    print("I am unable to connect to the database")

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

roadScores_tw = location_from_fuzzyMatch.roadScores_tw
roadScores_url = location_from_fuzzyMatch.roadScores_url
placeScores_tw = location_from_fuzzyMatch.placeScores_tw
placeScores_url = location_from_fuzzyMatch.placeScores_url
allScores = [roadScores_tw, roadScores_url, placeScores_tw, placeScores_url]

colNames = ['tw_road', 'url_road', 'tw_place', 'url_place']  # match with allScores

all_tid = collectTID(allScores)

sql = "insert into " + tb_in_Name + " values (%s, %s, %s)"

# insert all tid to the table first, can be used as key for later table update
for tid in range(len(all_tid)):
    try:
        cur.execute("insert into " + tb_in_Name + " (eid, tid) values (" + str(tid) + "," + str(all_tid[tid]) + ")")
        conn.commit()
    except:
        print("I can't insert tid into " + tb_in_Name)

# update table with scores based on tid
for scoreset in allScores:
    for score in scoreset:
        print("update " + tb_in_Name + " set " + colNames[
            allScores.index(scoreset)] + "=" + str(score[0]) + "where tid = " + str(score[-1]))
        try:
            cur.execute("update " + tb_in_Name + " set " + colNames[
                allScores.index(scoreset)] + "=" + str(score[0]) + "where tid = " + str(score[-1]))
            conn.commit()
        except:
            print("I can't insert score into " + tb_in_Name)

conn.close()
