"""
Created on  2/7/2019
@author: Jingchao Yang
"""
import pandas as pd
from psqlOperations import queryFromDB

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tb_in = 'original_gazetteer_power'
path_withClusters = 'D:\\harveyTwitter\\SaTScan_HouBry\\result\\result-Copy.txt'
path_withTIDs = 'D:\\harveyTwitter\\power_distribution_old_3.txt'
path_allIDs = 'D:\\harveyTwitter\\power_Clip_old2_HouBry_TableToExcel.csv'


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


def matchIDs(dpath, orgCluster):
    """

    :param dpath:
    :return:
    """
    data = pd.read_csv(dpath, header=None)
    # print(data)
    allClusters = []
    for c in orgCluster:
        tids = []
        for i in c[1]:
            tid = data.loc[data[0] == int(i) - 1]
            tids.append(str(tid.iloc[0][1]))
        allClusters.append((c[0], tids))
    return allClusters


def matchTIDs(dpath, orgCluster):
    """

    :param dpath:
    :return:
    """
    data = pd.read_csv(dpath)
    allClusters = []
    for c in orgCluster:
        tids, places, duplicate = [], [], []
        for i in c[1]:
            tid = data.loc[data['id'] == int(i)]
            tids.append(tid.iloc[0]['tid'])
            place = tid.iloc[0]['place']
            place = place.split(',')
            for p in place:
                if p is not None and p != '' and p.strip() not in duplicate:
                    places.append(p.strip())
                    duplicate.append(p.strip())
        allClusters.append((c[0], tids, places))
    return allClusters


# def matchGazetteer(col, tidList):
#     """
#
#     :param colName:
#     :param tidList:
#     :return:
#     """
#     tidList = ", ".join(str(x) for x in tidList)
#     sql = "select " + col + " from " + tb_in + " where tid in (" + tidList + ")"
#     gazetteerList = queryFromDB.freeQuery(dbConnect, sql)
#     return gazetteerList


'''get clusters'''
cluster_list = getCluster(path_withClusters)
allID_in_cluster = []
for cl in cluster_list:
    allID_in_cluster += cl[1]
allID_in_cluster = set(allID_in_cluster)
print('allID belong to cluster', allID_in_cluster)

'''get all IDs'''
allIDs = pd.read_csv(path_allIDs, header=None)
allIDs = allIDs[0]
IDList = allIDs.tolist()
IDList = [str(i + 1) for i in IDList]
IDSet = set(IDList)
print('all ID inputs', IDSet)

'''get non-clustered IDs'''
nonCluster_IDs = IDSet - allID_in_cluster
nonCluster_IDs = list(nonCluster_IDs)
print(len(IDSet), len(allID_in_cluster), len(nonCluster_IDs))
clusterID = len(cluster_list)
for nc in nonCluster_IDs:
    cluster_list.append((clusterID, [nc]))
    clusterID += 1

'''match IDs'''
print('start match location IDs to original IDs')
cluster_list1 = matchIDs(path_allIDs, cluster_list)
# for c in cluster_list1:
#     print(c)

'''match to original tids and places'''
print('start match original IDs to tids')
cluster_list2 = matchTIDs(path_withTIDs, cluster_list1)
for c in cluster_list2:
    print(c)

# '''match tids with gazetteer table'''
# print('start match tids with location names')
# gazetteer_list = []
# for cl2 in cluster_list2:
#     gazetteer = matchGazetteer('tw_road,tw_place,url_road,url_place', cl2[1])
#     duplicate, joint_gazetteer = [], []
#     for sub in gazetteer:
#         for subsub in sub:
#             try:
#                 allsub = subsub.split(',')
#                 allsub = [i.strip() for i in allsub]
#                 # allsub.strip()
#             except:
#                 allsub = [subsub]
#
#             for loc in allsub:
#                 if loc is not None and loc not in duplicate:
#                     joint_gazetteer.append(loc)
#                     duplicate.append(loc)
#     print((cl2[0], joint_gazetteer))
#     gazetteer_list.append((cl2[0], joint_gazetteer))
