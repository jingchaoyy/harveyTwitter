"""
Created on 6/13/2018
@author: Jingchao Yang
"""
from psqlOperations import psqlCollector

if __name__ == "__main__":
    dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
    tabName = "test"
    cloName = "ttext"
    data = psqlCollector.get_colData(dbConnect, tabName, cloName)
    for r in data:
        print(r)
