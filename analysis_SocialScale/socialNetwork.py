"""
Created on 10/3/2018
@author: Jingchao Yang
"""

from psqlOperations import queryFromDB
import csv

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tb_out_Event = "original_credibility_improved"
eid = 5536
tb_out_Original = "original"
tb_out_Tweets = "tweets"
csvfile = 'C:\\Users\\no281\\Documents\\harVeyTwitter\\results\\socialNetwork\\'


def getTIDs(eid, tbName):
    """
    Input event id from credibility table, and return a list of supporting tids

    :param eid: event id
    :param tbName: table name
    :return: list of supporting tids
    """
    sql = "select tids from " + tbName + " where eid = '" + str(eid) + "'"
    data = queryFromDB.freeQuery(dbConnect, sql)[0][0]
    if isinstance(data, str):
        data = data.split(', ')
    print(data)
    return data


def getUser(tid, tbName):
    """

    :param tid: one tid
    :param tbName: table name
    :return: associated user ID of the input tid
    """
    # sql = "select tu_id from " + tbName + " where tid = '" + str(tid) + "'"
    sql = "select tu_name from " + tbName + " where tid = '" + str(tid) + "'"
    userID = queryFromDB.freeQuery(dbConnect, sql)[0][0]
    return userID


def getRTs(tid, tbName):
    """
    find all the tids and users of associated retweets of the input tids of original twitter

    :param tid: tid of original twitter (supporting tweets under certain event)
    :param tbName: table name
    :return: tids of associated retweets
    """
    RTids = []
    sql = "select tid from " + tbName + " where t_reid = '" + str(tid) + "'"
    RTid = queryFromDB.freeQuery(dbConnect, sql)
    if len(RTid) > 0:
        for id in RTid:
            RTids.append(id[0])
    return RTids


def socialNet(supTIDs):
    """

    :param supTIDs:
    :return:
    """
    socialnet = []
    with open(csvfile + tb_out_Event + '_' + str(eid) + '.csv', "w", encoding='utf-8') as output:
        writer = csv.writer(output, lineterminator='\n')
        writer.writerow(('Source', 'Target'))
        for sup_id in supTIDs:
            user_ORI = getUser(sup_id, tb_out_Original)  # twitter user of a certain sup_id
            RTid_list = getRTs(sup_id, tb_out_Tweets)  # get all retweetIDs from a certain sup_id
            for id_RT in RTid_list:
                user_RT = getUser(id_RT, tb_out_Tweets)  # user that retweeted this certain sup_id
                socialnet.append((user_ORI, user_RT))

                writer.writerow((user_ORI, user_RT))
    return socialnet


supTIDs = getTIDs(eid, tb_out_Event)
network = socialNet(supTIDs)
print(network)
