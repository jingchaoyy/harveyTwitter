"""
Created on  2/7/2019
@author: Jingchao Yang
"""
import pandas as pd
from psqlOperations import queryFromDB
from datetime import date
from scipy.spatial import distance

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tb_in = 'original_gazetteer_power'
path_withClusters = 'D:\\harveyTwitter\\SaTScan_HouBry\\result3\\result-Copy.txt'
path_withTIDs = 'D:\\harveyTwitter\\power_distribution_old_3.txt'
path_allIDs = 'D:\\harveyTwitter\\power_distribution_old_2_2_C_TableToExcel.csv'


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
        cluster = ("".join(d.split('Coordinates / radius..:')[0].split('IDs included.:')[1].split())).split(',')
        certer = (d.split('Coordinates / radius..:')[1].split('Time frame............:')[0].split('/')[0]).split(',')
        lat = float(certer[0].split()[0].split('(')[1])
        lng = float(certer[1].split()[0].split(')')[0])
        radius = (d.split('Coordinates / radius..:')[1].split('Time frame............:')[0].split('/')[1]).split()[0]

        timeFrame = (d.split('Time frame............:')[1].split('Number of cases.......:')[0])
        sTime = timeFrame.split('to')[0].strip()
        sTime = [int(i) for i in sTime.split('/')]
        sTime = date(sTime[0], sTime[1], sTime[2])
        eTime = timeFrame.split('to')[1].strip()
        eTime = [int(i) for i in eTime.split('/')]
        eTime = date(eTime[0], eTime[1], eTime[2])
        clusters.append((id, cluster, (lat, -lng), radius, (sTime, eTime)))
        id += 1
    return clusters


def filter(dpath, orgCluster, lastID):
    """

    :param dpath:
    :return:
    """
    data = pd.read_csv(dpath)
    # print(data)
    allClusters = []
    for c in orgCluster[:72]:

        neighbors = []
        for i in c[1]:
            tid = data.loc[data['ori_id'] == int(i)]
            lat = tid.iloc[0]['lat']
            lng = tid.iloc[0]['lng']
            dst = distance.euclidean(c[2], (lat, lng)) * 100

            dtime = tid.iloc[0]['date'].split()[0]
            dtime = [int(i) for i in dtime.split('-')]
            ddtime = date(dtime[0], dtime[1], dtime[2])

            if (dst <= float(c[3]) + 0.5) and (
                    c[4][0] <= ddtime <= c[4][1]):  # within the distance range of 0.5 km buffer and also the time range
                neighbors.append(i)
            else:
                allClusters.append((lastID, [i]))
                lastID += 1

        allClusters.append((c[0], neighbors))
    for cc in orgCluster[72:]:
        allClusters.append(cc)

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
            else:
                pass
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
allIDs = pd.read_csv(path_allIDs)
allIDs = allIDs['id']
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
cluster_list1 = filter(path_allIDs, cluster_list, clusterID)
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
