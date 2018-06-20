"""
Created on 6/13/2018
@author: Jingchao Yang
"""
import geograpy
import geocoder
import re
from time import sleep
import googlemaps


def coorToLoc(coorList):  # geocoding coordinates, output locations (state)
    # coordinates: list of coordinates
    locations = []
    for coor in coorList:
        sleep(2)
        latlng = (coor[1], coor[2])
        # print(latlng)
        g = geocoder.google(latlng, method='reverse')
        # print(coor, g.state)
        locations.append((coor, g.state))
    return locations


def locFromText(set_Country, textList, filterList):
    # set_Country: define a country filter (one location name can associated with multiple countries
    # tw: Twitter text list selected from database
    loc = []
    print('Start extracting locations from texts')
    for t in textList:
        # print(row)
        text = t[1]
        if len(text) > 0:
            text = re.sub(r'[^\w]', ' ', text)  # remove symbol

            places = geograpy.get_place_context(text=text)
            addStr = places.address_strings
            for add in addStr:
                country = add.split(',')[2]  # get country name from extracted address_strings
                # print(country)
                if set_Country in country and not any(e in add for e in filterList):
                    # print('City:', add)
                    loc.append((t[0], add))
    return loc


def Remove(locList):
    final_list = []
    for loc in locList:
        if loc[1] not in final_list:
            final_list.append(loc[1])

    return final_list


def locToCoor(locList):  # geocoding locations, output coordinates
    # list of location names (e.g. 'City, State, Country')
    gmaps = googlemaps.Client(key='AIzaSyBNiwEzcU4-BPxp_cyoupC78ak_9PReeAY')
    coorFromText = []
    for loc in locList:
        sleep(2)
        g = gmaps.geocode(loc)

        if 'location' in g[0]['geometry'].keys():
            coor = g[0]['geometry']['location']  # APPROXIMATE location
            coor_Lat = coor['lat']
            coor_Lng = coor['lng']
        else:
            coor_Lat, coor_Lng = None, None

        if 'bounds' in g[0]['geometry'].keys():  # bounding box
            bbox = g[0]['geometry']['bounds']
            bbox_NE_Lat = bbox['northeast']['lat']
            bbox_NE_Lng = bbox['northeast']['lng']
            bbox_SW_Lat = bbox['southwest']['lat']
            bbox_SW_Lng = bbox['southwest']['lng']
        else:
            bbox_NE_Lat, bbox_NE_Lng, bbox_SW_Lat, bbox_SW_Lng = None, None, None, None
        # g = geocoder.google(loc)
        # print(loc, g.latlng)

        coors = (coor_Lat, coor_Lng, bbox_NE_Lat, bbox_NE_Lng, bbox_SW_Lat, bbox_SW_Lng)
        coorFromText.append((loc, coors))

    # g = geocoder.mapquest(locations, method='batch')

    return coorFromText


def coorToTexts(coorList, textList):
    # coor: non duplicate coordinate List
    # tw: tid with location
    # output: tid with coordinates if applicable
    tidWithCoor = []
    for t in textList:
        for coor in coorList:
            if t[1] == coor[0]:
                tidWithCoor.append((t[0], t[1], coor[1]))
    return tidWithCoor
