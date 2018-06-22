"""
Created on 6/20/2018
@author: Jingchao Yang

Correlation between user shared location and
tweets mentioned location with events analysis
"""
from psqlOperations import queryFromDB

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
user_coor_tb = "test_usercoor"
text_coor_tb = "test_textcoor"
event_tb = "test_events"

user_Coors = queryFromDB.get_allData(dbConnect, user_coor_tb)
print(user_Coors)
text_Coors = queryFromDB.get_allData(dbConnect, text_coor_tb)
print(text_Coors)
events = queryFromDB.get_allData(dbConnect, event_tb)
print(events)

joinByCoor = []
for e in events:
    for ucoor in user_Coors:
        for tcoor in text_Coors:
            if e[-1] == ucoor[-1] == tcoor[-1]:
                joinByCoor.append((e, ucoor, tcoor))
print('\nAfter Join\n', joinByCoor)
