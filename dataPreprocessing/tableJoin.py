"""
Created on  2/6/2019
@author: Jingchao Yang

Table join for one-to-one match post time and coordinates with tid
"""
from psqlOperations import queryFromDB
import csv

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"

tb_join1 = "original_credibility_power"
tb_join2 = "original_gazetteer_power"


def getTids(col):
    withTids = queryFromDB.get_multiColData(dbConnect, tb_join1, col)
    return withTids


def matchTime(col, tidList):
    """

    :param colName:
    :param tidList:
    :return:
    """
    sql = "select " + col + " from " + tb_join2 + " where tid in (" + tidList[-1] + ")"
    timeList = queryFromDB.freeQuery(dbConnect, sql)
    spaceTime = []
    for t in range(len(timeList)):
        if tidList[0] is not None and tidList[1] is not None and timeList[t][0] != '':
            spaceTime.append((tidList[0], tidList[1], timeList[t][0], str(timeList[t][1])))
    return spaceTime


colList = ['lat', 'lng', 'tids']
tidLists = getTids(colList)
# resultList = []
# for i in range(len(tidLists)):
#     result = matchTime('tcreate', tidLists[i])
#     resultList += result
#     # print(resultList)

path = 'D:\\harveyTwitter\\power_distribution_old_2.csv'
with open(path, "w", newline='', encoding="utf-8") as csv_file:
    writer = csv.writer(csv_file)
    id = 0
    for i in range(len(tidLists)):
        result = matchTime('tcreate, tid', tidLists[i])
        for line in result:
            print((id,) + line)
            writer.writerow((id,) + line)
            id += 1
csv_file.close()
