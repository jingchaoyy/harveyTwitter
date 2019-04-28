"""
Created on  4/26/2019
@author: Jingchao Yang

analyzing the correlation between the social impact (followers) of a user
with his degree centrality within graph (relation generated from socialNetwork.py)
"""
import pandas as pd
from psqlOperations import queryFromDB


def get_users(fname):
    """

    :param fname:
    :return: all user info from gephi output
    """
    data = pd.read_csv(fname)
    # u_name = data['Id']

    return data


def get_followers(dbc, uname_list):
    """

    :param dbc:
    :param uname_list: list of user names or ids
    :return: retrieve follower info from db
    """
    resultList = []
    count, listLen = 0, len(uname_list)
    for uname in uname_list:
        print(count, listLen)
        try:
            sql = "select max(tu_Followers) from tweets where tu_Name = '" + uname + "'"
            result = queryFromDB.freeQuery(dbc, sql)
            result = result[0][0]
            resultList.append((uname, result))
            count += 1
        except:
            print(uname)
            count += 1

    return resultList


dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"

file = 'D:\harveyTwitter\socialNetwork\gephi_graph.csv'
all_users = get_users(file)
unames = all_users['Id'].tolist()
degree_centrality = all_users[['Id', 'Degree']]
followers = get_followers(dbConnect, unames)
followers_df = pd.DataFrame(followers)
followers_df.columns = ['Id', 'followers']
export_csv = followers_df.to_csv(r'D:\harveyTwitter\socialNetwork\export_followers_all.csv', index=None, header=True)
