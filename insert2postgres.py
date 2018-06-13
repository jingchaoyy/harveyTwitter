"""
Created on 6/7/2018
@author: Jingchao Yang
"""

import os
import re
import ast
import simplejson as json
from datetime import datetime
from datetime import timedelta
import codecs
import psycopg2
import psycopg2.extras
from os import listdir
from os.path import isfile, join

mypath = 'C:\\Users\\no281\\Documents\\harVeyTwitter\\harvey_twitter_dataset\\02_archive_only\\subsets_30000'
onlyfiles = []
for f in listdir(mypath):
    if isfile(join(mypath, f)):
        onlyfiles.append(f)
print(onlyfiles)

try:
    conn = psycopg2.connect("dbname='harveyTwitts' user='postgres' host='localhost' password='123456'")
except:
    print("I am unable to connect to the database")

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

sql = "insert into tweets values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

# Read file and write file are separated
startpath = 'C:\\Users\\no281\\Documents\\harVeyTwitter\\harvey_twitter_dataset\\02_archive_only\\subsets_30000'

pr = onlyfiles

for f in pr:
    print(f)
    filename = f
    docpath_read = os.path.join(startpath, str(filename))
    doc_read = open(docpath_read, 'r')
    # pattern = re.compile("[\,\\t]")
    for tweet in doc_read.readlines():
        # suffix = "}}"
        # strT = str(tweet)
        # print(strT)
        # check = strT.endswith(suffix)
        # print(check)
        # if check == True:
        # if(tweet.endswith(suffix))

        try:
            if True:
                # new_tweet = ast.literal_eval(tweet.strip())
                new_tw = json.loads(tweet)

                tU_ID, tText, LatLon, tEM_url, tEM_murl, tEM_murls, tEM_ID, tEM_type, tEU_URL, \
                t_reCount, t_reID, tU_ID, tU_Geo, tU_Followers, tU_Desc, tU_Fri, tU_Name, tU_Loca, \
                tCreate = None, None, [None, None], None, None, None, None, None, None, \
                          None, None, None, None, None, None, None, None, None, None

                for attr, val in new_tw.items():

                    if attr == 'id':
                        try:
                            tID = val
                        except:
                            print('no id')

                    if attr == 'coordinates':
                        try:
                            for x, y in val.iteritems():
                                LatLon = y
                                if y != 'Point':
                                    coord = str(y[1]) + ',' + str(y[0])
                        except:
                            coord = ''

                    if attr == 'text':
                        tText = val
                        # tw = codecs.encode(val, 'utf-8')
                        tText = tText.replace(",", " ")
                        tText = tText.replace("\n", " ")

                    if attr == 'entities':
                        try:
                            for attr, val in val.items():
                                if attr == 'urls':
                                    if len(val) >0:
                                        try:
                                            for attr, val in val[0].items():
                                                if attr == 'url':
                                                    tEU_URL = str(val)

                                        except:
                                            print('entities urls reading error!')

                                if attr == 'media':
                                    if len(val) > 0:
                                        try:
                                            for attr, val in val[0].items():
                                                if attr == 'url':
                                                    tEM_url = str(val)
                                                if attr == 'media_url':
                                                    tEM_murl = str(val)
                                                if attr == 'media_url_https':
                                                    tEM_murls = str(val)
                                                if attr == 'id':
                                                    tEM_ID = val
                                                if attr == 'type':
                                                    tEM_type = str(val)
                                        except:
                                            print('entities media reading error!')

                        except:
                            print('entities reading error!')

                    if attr == 'retweet_count':
                        try:
                            t_reCount = val
                        except:
                            print('retweet_count reading error!')

                    if attr == 'retweeted_status':
                        try:
                            for attr, val in val.items():
                                if attr == 'id':
                                    t_reID = val
                        except:
                            print('retweeted_status reading error!')

                    if attr == 'user':
                        try:
                            for attr, val in val.items():
                                if attr == 'id':
                                    tU_ID = val
                                if attr == 'geo_enabled':
                                    tU_Geo = val
                                if attr == 'followers_count':
                                    tU_Followers = val
                                if attr == 'description':
                                    tU_Desc = val
                                    tU_Desc = tU_Desc.replace(",", " ")
                                    tU_Desc = tU_Desc.replace("\n", " ")
                                if attr == 'friends_count':
                                    tU_Fri = val
                                if attr == 'name':
                                    tU_Name = val
                                    tU_Name = tU_Name.replace(",", " ")
                                    tU_Name = tU_Name.replace("\n", " ")
                                if attr == 'location':
                                    tU_Loca = val
                                    tU_Loca = tU_Loca.replace(",", " ")
                                    tU_Loca = tU_Loca.replace("\n", " ")
                        except:
                            print('Tweet Encode Error!')

                    if attr == 'created_at':
                        try:
                            clean_timestamp = datetime.strptime(val, '%a %b %d %H:%M:%S +0000 %Y')
                            offset_hours = -5  # offset in hours for EST timezone is -5
                            local_timestamp = clean_timestamp + timedelta(hours=offset_hours)
                            tCreate = datetime.strftime(local_timestamp, '%Y-%m-%d %I:%M:%S %p')
                            Datetime = codecs.encode(val, 'utf-8')
                        except:
                            print('NO TIME')

                if tID:
                    data = (tID, tText, LatLon[1], LatLon[0], tEM_url, tEM_murl, tEM_murls, tEM_ID, tEM_type,
                            tEU_URL, t_reCount, t_reID, tU_ID, tU_Geo, tU_Followers, tU_Desc, tU_Fri, tU_Name,
                            tU_Loca, tCreate)

                    print(data)
                    try:
                        cur.execute(sql, data)
                        conn.commit()
                    except:
                        print("I can't insert into Test")
        except:
            raise
    doc_read.close()
conn.close()
