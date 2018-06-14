"""
Created on 6/13/2018
@author: Jingchao Yang
"""
from psqlOperations import queryClean
from psqlOperations import queryFromDB
import geograpy
import re

if __name__ == "__main__":
    dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
    tabName = "test"

    ############# Location from coordinates
    clo_Lat = "tlat"
    clo_Lon = "tlon"
    data = queryFromDB.get_coorData(dbConnect, tabName, clo_Lat, clo_Lon)
    print(data)

    ############ Location from text
    clo_Text = "ttext"
    data = queryClean.singleColumn_wFilter(dbConnect, tabName, clo_Text)

    locations = []
    for row in data:
        print(row)
        if len(row) > 0:
            places = geograpy.get_place_context(text=row)
            cities = places.cities
            print(cities)
            locations.append(cities)

    print(locations)

