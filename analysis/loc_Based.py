"""
Created on 6/13/2018
@author: Jingchao Yang
"""
from psqlOperations import psqlCollector

if __name__ == "__main__":
    dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
    tabName = "test"
    cloName1 = "tlat"
    cloName2 = "tlon"
    data = psqlCollector.get_coorData(dbConnect, tabName, cloName1, cloName2)
    print(data)
