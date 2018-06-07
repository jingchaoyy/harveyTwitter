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

mypath = 'G:\\data2'
onlyfiles = []
for f in listdir(mypath):
    if isfile(join(mypath, f)):
        onlyfiles.append(f)
print(onlyfiles)

try:
    conn = psycopg2.connect("dbname='twitter' user='postgres' host='localhost' password=''")
except:
    print("I am unable to connect to the database")
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
sql = "insert into information values (%s,%s, %s, %s,%s)"

# Read file and write file are separated
startpath = 'G:\\data2'

pr = onlyfiles

for f in pr:
    print(f)
    filename = f
    docpath_read = os.path.join(startpath, str(filename))
    doc_read = open(docpath_read, 'r')
    pattern = re.compile("[\,\\t]")
    for tweet in doc_read.readlines():
        suffix = "}}"
        check = str(tweet).endswith(suffix)
        print(check)
        # if check == True:
        # if(tweet.endswith(suffix))
        try:
            if check:
                new_tweet = ast.literal_eval(tweet.strip())
                new_tw = json.loads(json.dumps(new_tweet))
                for attr, val in new_tw.iteritems():
                    if attr == 'coordinates':
                        try:
                            for x, y in val.iteritems():
                                if y != 'Point':
                                    coord = str(y[1]) + ',' + str(y[0])
                        except:
                            coord = ''
                            break
                    if attr == 'text':
                        tw = codecs.encode(val, 'utf-8')
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
                            for attr, val in val.iteritems():
                                if attr == 'id':
                                    Twt = str(val)
                        except:
                            print('Tweet Encode Error!')
                            continue
                if coord:
                    data = (Twt, float(y[1]), float(y[0]), final_timestamp, tw)
                    print(data)
                    try:
                        cur.execute(sql, data)
                        conn.commit()
                    except:
                        print("I can't insert into information")
        except:
            raise
    doc_read.close()
conn.close()

