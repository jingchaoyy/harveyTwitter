"""
Created on 7/5/2018
@author: Jingchao Yang

Goal: insert local gazetteer match scores to database
"""
import psycopg2.extras
from dataPreprocessing import gazetteer_from_fuzzyMatch
from toolBox import fuzzy_gazatteer
from psqlOperations import queryFromDB


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


''' db connection '''
dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tb_in_Name = 'original_gazetteer_shelter2'  # table to store gazetteers and associated ground truth match score with tid
try:
    conn = psycopg2.connect("dbname='harveyTwitts' user='postgres' host='localhost' password='123456'")
except:
    print("I am unable to connect to the database")
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

''' fuzzy gazetteers, getting gazetteers from text (tws/ urls) '''
roads_from_tw = gazetteer_from_fuzzyMatch.roads_from_tw
roads_from_url = gazetteer_from_fuzzyMatch.roads_from_url
places_from_tw = gazetteer_from_fuzzyMatch.places_from_tw
places_from_url = gazetteer_from_fuzzyMatch.places_from_url
allGazetteers = [roads_from_tw, places_from_tw, roads_from_url, places_from_url]

# ''' match scores between gazetteers from text and ground truth '''
# roads_from_tru = gazetteer_from_fuzzyMatch.roads_from_tru
# places_from_tru = gazetteer_from_fuzzyMatch.places_from_tru
#
# roadScores_tw = fuzzy_gazatteer.fuzzyLocMatch_wGT(roads_from_tw, roads_from_tru)
# roadScores_url = fuzzy_gazatteer.fuzzyLocMatch_wGT(roads_from_url, roads_from_tru)
# placeScores_tw = fuzzy_gazatteer.fuzzyLocMatch_wGT(places_from_tw, places_from_tru)
# placeScores_url = fuzzy_gazatteer.fuzzyLocMatch_wGT(places_from_url, places_from_tru)
# allScores = [roadScores_tw, placeScores_tw, roadScores_url, placeScores_url]

''' check is tb exist, if yes, drop and recreate '''
try:
    cur.execute("drop table " + tb_in_Name)
    conn.commit()
    print("drop table succeeded " + tb_in_Name)
except:
    print("drop table failed " + tb_in_Name)
    conn.rollback()  # when command fail, the transaction will be aborted and no further command will be executed
    # until a call to the rollback(). This except will prevent such abort when table is new and cannot be found and drop

try:
    cur.execute("create table " + tb_in_Name + "(eID int PRIMARY KEY NOT NULL,"
                                               "tw_road Text,"
                                               "tw_place Text,"
                                               "url_road Text,"
                                               "url_place Text,"
                                               # "tw_road_score double precision,"
                                               # "tw_place_score double precision,"
                                               # "url_road_score double precision,"
                                               # "url_place_score double precision,"
                                               "tcreate Text,"
                                               "tID bigint);")
    conn.commit()
    print("create table succeeded " + tb_in_Name)
except:
    print("create table failed " + tb_in_Name)

''' insert all tid into the table first, can be used as key for later table update '''
all_tid = collectTID(allGazetteers)
for tid in range(len(all_tid)):
    try:
        cur.execute("insert into " + tb_in_Name + " (eid, tid) values (" + str(tid) + "," + str(all_tid[tid]) + ")")
        conn.commit()
    except:
        print("I can't insert tid into " + tb_in_Name)

''' update table with gazetteers based on tid '''
gz_colNames = ['tw_road', 'tw_place', 'url_road', 'url_place']
for gazetteers in allGazetteers:
    for gz in gazetteers:
        print("update " + tb_in_Name + " set " + gz_colNames[
            allGazetteers.index(gazetteers)] + " = '" + ', '.join(gz[0]) + "' where tid = " + str(gz[-1]))
        try:
            cur.execute("update " + tb_in_Name + " set " + gz_colNames[
                allGazetteers.index(gazetteers)] + " = '" + ', '.join(gz[0]) + "' where tid = " + str(gz[-1]))
            conn.commit()
        except:
            print("I can't insert gz into " + tb_in_Name)

# ''' update table with scores based on tid '''
# score_colNames = ['tw_road_score', 'tw_place_score', 'url_road_score', 'url_place_score']  # match with allScores
# for scoreset in allScores:
#     for score in scoreset:
#         print("update " + tb_in_Name + " set " + score_colNames[
#             allScores.index(scoreset)] + " = " + str(score[0]) + " where tid = " + str(score[-1]))
#         try:
#             cur.execute("update " + tb_in_Name + " set " + score_colNames[
#                 allScores.index(scoreset)] + " = " + str(score[0]) + " where tid = " + str(score[-1]))
#             conn.commit()
#         except:
#             print("I can't insert score into " + tb_in_Name)

''' update table with tweet time based on tid '''
tw_tb = "original"
tb_col = "tcreate"
post_time = queryFromDB.get_colData(dbConnect, tw_tb, tb_col)

time_col = 'tcreate'
for time in post_time:
    print("update " + tb_in_Name + " set " + time_col + " = '" + str(time[1])
          + "' where tid = " + str(time[0]))
    try:
        cur.execute("update " + tb_in_Name + " set " + time_col + " = '" + str(time[1])
                    + "' where tid = " + str(time[0]))
        conn.commit()
    except:
        print("I can't insert time into " + tb_in_Name)

conn.close()
