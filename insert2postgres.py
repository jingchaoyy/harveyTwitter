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

mypath = 'C:\\Users\\no281\\Documents\\harVeyTwitter\\harvey_twitter_dataset\\02_archive_only\\testSubset'
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

sql = "insert into test values (%s, %s, %s, %s, %s, %s)"

# Read file and write file are separated
startpath = 'C:\\Users\\no281\\Documents\\harVeyTwitter\\harvey_twitter_dataset\\02_archive_only\\testSubset'

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
                uid, y, final_timestamp = None, [], None
                for attr, val in new_tw.items():

                    if attr == 'id':
                        try:
                            tid = val
                        except:
                            print('no id')

                    if attr == 'coordinates':
                        try:
                            for x, y in val.items():
                                print('$$$$$',y)
                                if y != 'Point':
                                    coord = str(y[1]) + ',' + str(y[0])
                        except:
                            coord = ''
                            break

                    if attr == 'text':
                        tw = val
                        # tw = codecs.encode(val, 'utf-8')
                        tw = tw.replace(",", " ")
                        tw = tw.replace("\n", " ")

                    if attr == 'created_at':
                        try:
                            clean_timestamp = datetime.strptime(val, '%a %b %d %H:%M:%S +0000 %Y')
                            offset_hours = -5  # offset in hours for EST timezone is -5
                            local_timestamp = clean_timestamp + timedelta(hours=offset_hours)
                            final_timestamp = datetime.strftime(local_timestamp, '%Y-%m-%d %I:%M:%S %p')
                            Datetime = codecs.encode(val, 'utf-8')
                        except:
                            print('NO TIME')
                            break

                    if attr == 'user':
                        try:
                            for attr, val in val.items():
                                if attr == 'id':
                                    uid = str(val)
                        except:
                            print('Tweet Encode Error!')
                            continue



                if tid:
                    if len(y) == 2:
                        data = (tid, uid, float(y[1]), float(y[0]), final_timestamp, tw)
                    else:
                        data = (tid, uid, None, None, final_timestamp, tw)
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
