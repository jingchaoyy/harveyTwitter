"""
Created on 6/13/2018
@author: Jingchao Yang
"""
import geograpy
# import geocoder
import re
from time import sleep
import googlemaps
import math

gmaps = googlemaps.Client(key='AIzaSyByJv_mMoTZ9DvB4XeH3Zs4zYg4cKEg2qU')


# def coorToLoc(coorList):  # geocoding coordinates, output locations (state)
#     # coordinates: list of coordinates
#     locations = []
#     for coor in coorList:
#         sleep(2)
#         latlng = (coor[1], coor[2])
#         # print(latlng)
#         g = geocoder.google(latlng, method='reverse')
#         # print(coor, g.state)
#         locations.append((coor, g.state))
#     return locations


def locFromText(set_Country, textList, filterList):
    """
    Extract location from twitters
    :param set_Country: define a country filter (one location name can associated with multiple countries
    :param textList: A list of all Twitter text selected from database
    :param filterList: Name list that should not be considered as location under certain event
    :return: A filtered list of location extracted from Twitter text
    """
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
    """
    remove duplicated location extracted from twitters to prepare for geocoding
    :param locList: original location list for all twitters (if there is one)
    :return: non duplicate location List
    """
    final_list = []
    for loc in locList:
        if loc[1] not in final_list:
            final_list.append(loc[1])

    return final_list


def locToCoor(locList):  # geocoding locations, output coordinates
    """
    geocoding, assign coor to location
    :param locList: non duplicate location List (e.g. 'City, State, Country')
    :return: non duplicate location List with assigned coors
    """
    print('start assign goor to location')
    coorFromText = []
    for loc in locList:
        print(loc)
        coors = roadToCoor(loc)[1]
        print(coors)
        coorFromText.append((loc, coors))

    # g = geocoder.mapquest(locations, method='batch')

    return coorFromText


def coorToTexts(coorList, textList):
    """
    assign geocoded coors back to all twitters
    :param coorList: non duplicate coordinate List associated with extracted location
    :param textList: tid with location
    :return: tid with coordinates if applicable
    """
    tidWithCoor = []
    for t in textList:
        for coor in coorList:
            if t[1] == coor[0]:
                tidWithCoor.append((t[0], t[1], coor[1]))
    return tidWithCoor


def locationFilter(locList, filterList, set_Country):  # filter out location out of certain region
    """
    Filter out url extracted locations with desired country
    :param locList: A list of all url extracted location names
    :param filterList: Name list that should not be considered as location under certain event
    :param set_Country: define a country filter (one location name can associated with multiple countries
    :return: a filtered location list
    """
    print('start filter location')
    filteredLoc = []
    for loc in locList:
        print(loc[0])
        if len(loc) > 0:
            for l in loc[1]:
                country = l.split(',')[2]
                if set_Country in country and not any(e in l for e in filterList):
                    print(l)
                    filteredLoc.append((loc[0], l))
    return filteredLoc


def placeToRoad(placeName):
    """
    Geocoding, assign road name to place name

    :param placeName: place name (e.g. 'San Marcos Activity Center')
    :return: associated road name, postal code and coordinates
    """
    # sleep(2)
    g = gmaps.geocode(placeName)
    roadNo, roadName = '', ''
    zipCode, coor_Lat, coor_Lng = None, None, None
    if len(g) > 0:
        for ac in g[0]['address_components']:
            if ac['types'] and len(ac['types']) > 0:
                if ac['types'][0] == 'street_number':
                    try:
                        roadNo = ac['long_name']
                    except:
                        roadNo = ''
                if ac['types'][0] == 'route':
                    try:
                        roadName = ac['long_name']
                    except:
                        roadName = ''
                if ac['types'][0] == 'postal_code':
                    try:
                        zipCode = ac['long_name']
                    except:
                        zipCode = None

        # if 'long_name' in g[0]['address_components'][0].keys():  # road no.
        #     if g[0]['address_components'][0]['types'][0] == 'street_number':
        #         try:
        #             roadNo = g[0]['address_components'][0]['long_name']
        #         except:
        #             roadNo = ''
        #
        # if 'long_name' in g[0]['address_components'][1].keys():  # road name
        #     if g[0]['address_components'][1]['types'][0] == 'route':
        #         try:
        #             roadName = g[0]['address_components'][1]['long_name']
        #         except:
        #             roadName = ''
        #
        # if 'long_name' in g[0]['address_components'][-1].keys():  # zip code
        #     if g[0]['address_components'][-1]['types'][0] == 'postal_code':
        #         try:
        #             zipCode = g[0]['address_components'][-1]['long_name']
        #         except:
        #             zipCode = None

        if 'location' in g[0]['geometry'].keys():
            try:
                coor = g[0]['geometry']['location']  # APPROXIMATE location
                coor_Lat = coor['lat']
                coor_Lng = coor['lng']
            except:
                coor_Lat, coor_Lng = None, None

    roadName = roadNo + ' ' + roadName
    coor = (coor_Lat, coor_Lng)
    return roadName, zipCode, coor


def roadToCoor(rn):
    """
    Provide associated zipcode and coordinates

    :param rn: road name
    :return: coor (lat, lng) and associated coordinates
    """
    # sleep(2)
    g = gmaps.geocode(rn)

    zipCode = None
    coor_Lat, coor_Lng, bbox_NE_Lat, bbox_NE_Lng, bbox_SW_Lat, bbox_SW_Lng = None, None, None, None, None, None
    if len(g) > 0:
        if len(g) > 0:
            for ac in g[0]['address_components']:
                try:
                    if ac['types'][0] == 'postal_code':
                        zipCode = ac['long_name']
                except:
                    zipCode = None

        if 'location' in g[0]['geometry'].keys():
            try:
                coor = g[0]['geometry']['location']  # APPROXIMATE location
                coor_Lat = coor['lat']
                coor_Lng = coor['lng']
            except:
                coor_Lat, coor_Lng = None, None

        if 'bounds' in g[0]['geometry'].keys():  # bounding box
            try:
                bbox = g[0]['geometry']['bounds']
                bbox_NE_Lat = bbox['northeast']['lat']
                bbox_NE_Lng = bbox['northeast']['lng']
                bbox_SW_Lat = bbox['southwest']['lat']
                bbox_SW_Lng = bbox['southwest']['lng']
            except:
                bbox_NE_Lat, bbox_NE_Lng, bbox_SW_Lat, bbox_SW_Lng = None, None, None, None

    # g = geocoder.google(loc)
    # print(loc, g.latlng)
    coors = (coor_Lat, coor_Lng, bbox_NE_Lat, bbox_NE_Lng, bbox_SW_Lat, bbox_SW_Lng)
    return zipCode, coors


def eucDist(coor1, coor2):
    """

    :param coor1: (lat1, lng1)
    :param coor2: (lat2, lng2)
    :return: euclidean distance
    """
    x1 = float(coor1[1])
    y1 = float(coor1[0])
    x2 = float(coor2[1])
    y2 = float(coor2[0])

    dist = math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)
    return dist
