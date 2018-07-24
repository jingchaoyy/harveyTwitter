"""
Created on 7/5/2018
@author: Jingchao Yang
"""
from psqlOperations import queryFromDB
from toolBox import events_from_tweets

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"


def selfCrediable(tw_gz, url_gz):
    """
    Check if same extracted gazetteer shows both in user tweets and in urls, if yes, give credits
    Based on original_gazetteer table and compare with original_credibility_improved table, since corelated roads
    and places are already grouped  using geocoding in original_credibility_improved table.

    :param tw_gz: gazetteer from tws
    :param url_gz: gazetteer from urls
    :return: credibility after self evaluated
    """
    tb_out_Name = "original_credibility_improved"
    tb_query_Col = 'tids'
    creditList = []
    for tgz in tw_gz:
        credit = 0
        if tgz[0] != ', ':  # credit is 0 when no matches, no need to go though the rest
            gzs = events_from_tweets.remove(tgz[0].split(','))
            tid = [str(tgz[-1])]
            events = queryFromDB.likeQuery_all(dbConnect, tb_out_Name, tb_query_Col, tid)  # get data from
            # original_credibility_improved table, work as a local gazetteer db, collect eid as gazetteer reference No.
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

            eids = events_from_tweets.remove(eids)  # keep only non duplicated gazetteer reference No. (eids)
            if len(eids) > 0:  # when matches found in user tweets, meaning there's a chance same location also in urls
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
                credit = len(
                    set([i for i in eids if eids.count(i) > 1]))  # get eids that shown in the list more than once,
                # meaning found both in tw and in url
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
