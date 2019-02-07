"""
Created on  2/7/2019
@author: Jingchao Yang
"""
import pandas as pd
from psqlOperations import queryFromDB

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tb_in = 'original_gazetteer_power'
path = 'D:\\harveyTwitter\\SaTScan_HouBry\\result\\result-Copy.txt'
path_csv = 'D:\\harveyTwitter\\power_distribution_old_2.txt'


def getCluster(dpath):
    """

    :param dpath:
    :return:
    """
    f = open(dpath, 'r')
    data = f.read()
    data = data.split('\n\n')

    clusters = []
    id = 0
    for d in data:
        cluster = ("".join(d.split('Coordinates')[0].split('IDs included.:')[1].split())).split(',')
        clusters.append((id, cluster))
        id += 1
    return clusters


def matchTIDs(dpath, orgCluster):
    """

    :param dpath:
    :return:
    """
    data = pd.read_csv(dpath)
    allClusters = []
    for c in orgCluster:
        tids = []
        for i in c[1]:
            tid = data.loc[data['id'] == int(i)]
            tids.append(tid.iloc[0]['tid'])
        allClusters.append((c[0], tids))
    return allClusters


def matchGazetteer(col, tidList):
    """

    :param colName:
    :param tidList:
    :return:
    """
    tidList = ", ".join(str(x) for x in tidList)
    sql = "select " + col + " from " + tb_in + " where tid in (" + tidList + ")"
    gazetteerList = queryFromDB.freeQuery(dbConnect, sql)
    return gazetteerList


'''get clusters'''
cluster_list = getCluster(path)
# for c in cluster_list:
#     print(c)

'''match to original tids'''
cluster_list2 = matchTIDs(path_csv, cluster_list)
for c in cluster_list2:
    print(c)

'''match tids with gazetteer table'''
gazetteer_list = []
for cl in cluster_list2:
    gazetteer = matchGazetteer('tw_road,tw_place,url_road,url_place', cl[1])
    duplicate, joint_gazetteer = [], []
    for sub in gazetteer:
        for subsub in sub:
            try:
                allsub = subsub.split(',')
                allsub = [i.strip() for i in allsub]
                # allsub.strip()
            except:
                allsub = [subsub]

            for loc in allsub:
                if loc is not None and loc not in duplicate:
                    joint_gazetteer.append(loc)
                    duplicate.append(loc)

    gazetteer_list.append((cl[0], joint_gazetteer))

for gl in gazetteer_list:
    print(gl)
