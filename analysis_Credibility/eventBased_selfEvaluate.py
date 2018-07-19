"""
Created on 7/5/2018
@author: Jingchao Yang
"""
from psqlOperations import queryFromDB
from toolBox import events_from_tweets

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"


def selfCrediable(tw_gz, url_gz):
    tb_out_Name = "original_credibility_improved"
    tb_query_Col = 'tids'
    creditList = []
    for tgz in tw_gz:
        credit = 0
        if tgz[0] != ', ':
            gzs = events_from_tweets.remove(tgz[0].split(','))
            tid = [str(tgz[-1])]
            events = queryFromDB.likeQuery_all(dbConnect, tb_out_Name, tb_query_Col, tid)
            eids = []
            for event in events:
                for gz in gzs:  # for each gz under same tid
                    gz = gz.strip()
                    if gz != '':
                        if gz.startswith('*R '):
                            if gz in event[1]:
                                eids.append(event[0])
                        if gz.startswith('#p '):
                            if gz in event[2]:
                                eids.append(event[0])

            eids = events_from_tweets.remove(eids)
            if len(eids) > 0:
                events_1 = []
                for event in events:
                    if event[0] in eids:
                        events_1.append(event)

                for ugz in url_gz:
                    if ugz[-1] == tgz[-1]:
                        for event1 in events_1:
                            gzs = events_from_tweets.remove(ugz[0].split(','))
                            for gz in gzs:  # for each gz under same tid
                                gz = gz.strip()
                                if gz != '':
                                    if gz.startswith('*R '):
                                        if gz in event1[1]:
                                            eids.append(event1[0])
                                    if gz.startswith('#p '):
                                        if gz in event1[2]:
                                            eids.append(event1[0])
                credit = len(eids) - len(events_from_tweets.remove(eids))
        print((credit, tgz[-1]))
        creditList.append((credit, tgz[-1]))
    return creditList


'''databsed connection variables'''

gz_tb = "original_gazetteer"
col1 = "tw_road"
col2 = "tw_place"
col3 = "url_road"
col4 = "url_place"

tw_gz1 = queryFromDB.mergeSelect(dbConnect, gz_tb, col1, col2)
url_gz1 = queryFromDB.mergeSelect(dbConnect, gz_tb, col3, col4)
evaluate_score = selfCrediable(tw_gz1, url_gz1)
# for i in evaluate_score:
#     print(i)
