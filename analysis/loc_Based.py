"""
Created on 6/13/2018
@author: Jingchao Yang
"""
from psqlOperations import queryClean
from psqlOperations import queryFromDB
import geograpy
import geocoder
import re
from time import sleep


def coorToLoc(coorList):  # geocoding coordinates, output locations (state)
    # coordinates: list of coordinates
    locations = []
    for coor in coorList:
        sleep(5)
        latlng = (coor[1], coor[2])
        # print(latlng)
        g = geocoder.google(latlng, method='reverse')
        # print(coor, g.state)
        locations.append((coor, g.state))
    return locations


def locFromText(set_Country, twList):
    # set_Country: define a country filter (one location name can associated with multiple countries
    # tw: Twitter text list selected from database
    loc = []
    print('Start extracting locations from texts')
    for tw in twList:
        # print(row)
        text = tw[1]
        if len(text) > 0:
            text = re.sub(r'[^\w]', ' ', text)  # remove symbol

            places = geograpy.get_place_context(text=text)
            addStr = places.address_strings
            for add in addStr:
                country = add.split(',')[2]  # get country name from extracted address_strings
                # print(country)
                if set_Country in country:
                    # print('City:', add)
                    loc.append((tw[0], add))
    return loc


def Remove(locList, filterList):
    final_list = []
    for loc in locList:
        if loc[1] not in final_list and not any(e in loc[1] for e in filterList):
            final_list.append(loc[1])

    return final_list


def locToCoor(locList):  # geocoding locations, output coordinates
    # list of location names (e.g. 'City, State, Country')
    coorFromText = []
    for loc in locList:
        sleep(5)
        g = geocoder.google(loc)
        # print(loc, g.latlng)
        coorFromText.append((loc, g.latlng))

    # g = geocoder.mapquest(locations, method='batch')

    return coorFromText


def coorToTweets(coorList, twList):
    # coor: non duplicate coordinate List
    # tw: tid with location
    # output: tid with coordinates if applicable
    tidWithCoor = []
    for tw in twList:
        for coor in coorList:
            if tw[1] == coor[0]:
                tidWithCoor.append((tw[0], coor[1]))
    return tidWithCoor


if __name__ == "__main__":
    dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
    tabName = "test"

    # ############# Location from coordinates
    clo_Lat = "tlat"
    clo_Lon = "tlon"
    data_coor = queryFromDB.get_coorData(dbConnect, tabName, clo_Lat, clo_Lon)
    print(data_coor)
    # locFromCoor = coorToLoc(data_coor)
    # print('Coordinates with State', locFromCoor)

    ############ Location from text
    clo_Text = "ttext"
    data_text = queryClean.singleColumn_wFilter(dbConnect, tabName, clo_Text)
    print('Original English Only Tweets', data_text)

    setCountry = 'United States'
    loc_fromText = locFromText(setCountry, data_text)
    print('All locations extracted', loc_fromText)

    locFilter = ['Harvey', 'Hurricane']  # Name list that should not be considered as location under certain event
    loc_nonDup = Remove(loc_fromText, locFilter)
    print('Non duplicate location list', loc_nonDup)

    coorFromLoc_nonDup = locToCoor(loc_nonDup)
    print('Associated coordinates', coorFromLoc_nonDup)

    allCoors = coorToTweets(coorFromLoc_nonDup, loc_fromText)
    print(allCoors)
