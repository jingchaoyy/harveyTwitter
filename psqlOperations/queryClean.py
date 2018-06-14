"""
Created on 6/14/2018
@author: Jingchao Yang
"""
from psqlOperations import queryFromDB
import re

def singleColumn_nonFilter(dbConnect, tabName, cloName):
    data = queryFromDB.get_colData_Eng(dbConnect, tabName, cloName)
    rows = []
    for row in data:
        row = row.strip()
        rows.append(row)

    return rows

def singleColumn_wFilter(dbConnect, tabName, cloName):
    data = queryFromDB.get_colData_Eng(dbConnect, tabName, cloName)
    rows = []
    for row in data:
        row = re.sub(r'[^\w]', ' ', row)  # symbol filter
        row = re.sub(r'\s+', ' ', row)  # remove extra space between words
        row = row.strip()
        rows.append(row)

    return rows