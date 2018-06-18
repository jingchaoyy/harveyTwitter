"""
Created on 6/13/2018
@author: Jingchao Yang
"""
from psqlOperations import queryClean
from psqlOperations import queryFromDB
import geograpy
import geocoder
import googlemaps


if __name__ == "__main__":
    dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
    tabName = "test"

    ############# Location from coordinates
    clo_Lat = "tlat"
    clo_Lon = "tlon"
    data = queryFromDB.get_coorData(dbConnect, tabName, clo_Lat, clo_Lon)
    for coor in data:
        print(coor)

    ############ Location from text
    clo_Text = "ttext"
    data = queryClean.singleColumn_wFilter(dbConnect, tabName, clo_Text)

    setCountry = 'United States'
    locFromText, coorFromText = [], []
    for row in data:
        # print(row)
        if len(row) > 0:
            places = geograpy.get_place_context(text=row)
            cities = places.address_strings
            for city in cities:
                country = city.split(',')[2]  # get country name from extracted address_strings
                # print(country)
                if setCountry in country:
                    print(city)
                    locFromText.append(city)
                    g = geocoder.google(city)
                    print(g.latlng)
                    coorFromText.append(g.latlng)


    print(locFromText)
    print(coorFromText)
