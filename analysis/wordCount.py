"""
Created on 6/13/2018
@author: Jingchao Yang
"""
from psqlOperations import queryClean

if __name__ == "__main__":
    dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
    tabName = "test"
    cloName = "ttext"

    resuls = queryClean.singleColumn_wFilter(dbConnect, tabName, cloName)
    for i in resuls:
        print(i)

