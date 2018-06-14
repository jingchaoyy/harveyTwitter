"""
Created on 6/13/2018
@author: Jingchao Yang
"""
from psqlOperations import psqlCollector
import geograpy
import re

if __name__ == "__main__":
    dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
    tabName = "test"

    ############# Location from coordinates
    # clo_Lat = "tlat"
    # clo_Lon = "tlon"
    # data = psqlCollector.get_coorData(dbConnect, tabName, clo_Lat, clo_Lon)
    # print(data)

    ############# Location from text
    clo_Text = "ttext"
    data = psqlCollector.get_colData_Eng(dbConnect, tabName, clo_Text)

    locations = []
    for row in data:
        row = re.sub(r'[^\w]', ' ', row)
        row = re.sub(r'\s+', ' ', row)
        row = row.strip()
        print(row)

        if len(row) > 0:
            places = geograpy.get_place_context(text=row)
            cities = places.cities
            locations.append(cities)

    print(locations)




    # strr = 'Virginia, United States is where I live'
    # places = geograpy.get_place_context(text=strr)
    # con = places.countries
    # print(con)
