"""
Created on 6/29/2018
@author: Jingchao Yang

Goal: Comparing local gazetteer information extracted from tws with direct from ground truth sources
"""
from psqlOperations import queryFromDB


def scoreEva(scoreList):
    """
    Putting all scores into consideration, including scores calculated for :
    road extracted from tw
    place extracted from tw
    road extracted from url
    place extracted from url

    :param scoreList: all scores directly from score table
    :return: single final score to represent that twitter, possible maximum = 1.4
    """
    finals = []
    for score in scoreList:
        tw_road = score[1]
        tw_place = score[2]
        url_road = score[3]
        url_place = score[4]
        scores = [tw_road, tw_place, url_road, url_place]
        scores = [x for x in scores if x is not None]
        finalSum = max(scores)
        if scores is not None:
            restSum = (sum(scores))
            finalSum = finalSum + (restSum - finalSum) * 0.1
        finals.append((finalSum, score[-1]))
    return finals


dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tb1_out_Name = "test_matchscores"
allScores = queryFromDB.get_allData(dbConnect, tb1_out_Name)
evaScores = scoreEva(allScores)

for i in evaScores:
    print(i)
