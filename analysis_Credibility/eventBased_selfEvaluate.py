"""
Created on 7/24/2018
@author: Jingchao Yang
"""

from psqlOperations import queryFromDB
from toolBox import events_from_tweets

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"


def assignCredit(events):
    """
    Each event supports by multiple tweets, when a supporting tweet have this event in its tw extracted gazetteer,
    credibility count 1, and count another 1 if this event also in its url text extracted gazetteer. Average this
    location-based event credibility by the source count (in our case, tw and url, together is 2). A supporting tweet
    may also get retweeted, get the retweet count and normalize by dividing max possible retweet count(in the whole db).
    Sum up all location-based event credibility and retweet-based event credibility after looping though all supporting
    tids, and assign these two credit to the event with its eid in original_credibility_improved table

    :param events: event list with ['eid', 'road_events', 'place_events', 'tids']
    :return: credibility based on correponding tids
    """
    tb2_out_Name = "original_gazetteer"
    tb3_out_Name = "original"
    col = "tid"
    eventCredits = []
    resourceType = ['tw', 'url']  # sources list

    ''' getting max re tweet number from whole database, for later normalization '''
    sql = 'select max(t_recount) from ' + tb3_out_Name
    maxRT = queryFromDB.freeQuery(dbConnect, sql)[0][0]
    print('Max re tweet number:', maxRT)

    for event in events:
        eid = event[0]
        tids = event[-1]
        tids = tids.split(', ')
        roadEvents, placeEvents = [], []
        if event[1] is not None:
            roadEvents = events_from_tweets.remove(event[1].split(', '))
        if event[2] is not None:
            placeEvents = events_from_tweets.remove(event[2].split(', '))
        locCredit, rtCredit = [], []
        for tid in tids:

            ''' get retweet number for the corresponding tid '''
            ori = queryFromDB.query(dbConnect, tb3_out_Name, col, tid)
            ori = ori[0]
            rT = ori[10]  # column: t_recount, for retweet count
            rtCredit.append(rT)

            ''' get extracted gazetteers for the corresponding tid '''
            tw_roads, tw_places, url_roads, url_places = [], [], [], []
            gazetteer = queryFromDB.query(dbConnect, tb2_out_Name, col, tid)
            gazetteer = gazetteer[0]
            if gazetteer[1] is not None:
                tw_roads = events_from_tweets.remove(gazetteer[1].split(', '))
                # print(tw_roads)
            if gazetteer[2] is not None:
                tw_places = events_from_tweets.remove(gazetteer[2].split(', '))
                # print(tw_places)
            if gazetteer[3] is not None:
                url_roads = events_from_tweets.remove(gazetteer[3].split(', '))
                # print(url_roads)
            if gazetteer[4] is not None:
                url_places = events_from_tweets.remove(gazetteer[4].split(', '))
                # print(url_places)

            ''' see if any roads from tw matches, if not, check places. Then for the urls
            For each tid, max location match score is 1 + 1 = 2 '''
            if any(twr in roadEvents for twr in tw_roads):
                locCredit.append(1)
                # print('aaaa')
            elif any(twp in placeEvents for twp in tw_places):
                locCredit.append(1)
                # print('bbbb')

            if any(urlr in roadEvents for urlr in url_roads):
                locCredit.append(1)
                # print('cccc')
            elif any(urlp in placeEvents for urlp in url_places):
                locCredit.append(1)
                # print('dddd')

        print((eid, sum(locCredit) / len(resourceType), sum(rtCredit) / maxRT))
        eventCredits.append((eid, sum(locCredit) / len(resourceType), sum(rtCredit) / maxRT))
    return eventCredits


tb1_out_Name = "original_credibility_improved"
colList1 = ['eid', 'road_events', 'place_events', 'tids']

data_events = queryFromDB.get_multiColData(dbConnect, tb1_out_Name, colList1)
creditList = assignCredit(data_events)
