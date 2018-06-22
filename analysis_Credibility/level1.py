"""
Created on 6/22/2018
@author: Jingchao Yang

Level One credibility test include basic location based cross check
Location sources includes:
User shared location
Twitter embedded text location extraction
Twitter linked web page text location extraction

Goal is to analysis if coordinates/ locations from same twitter are correlated (same, fall within, near by?)
"""
from psqlOperations import queryFromDB


def locExtraction_2(locList1, locList2):
    """if all two out of three location info can be found within same tweets
    Meaning if records from two tables share the same tid"""
    joinByCoor = []
    for loc1 in locList1:
        for loc2 in locList2:
            if loc1[-1] == loc2[-1]:
                joinByCoor.append((loc1, loc2))
    return joinByCoor


def locExtraction_3(locList1, locList2, locList3):
    """if all three location info can be found within same tweets
    Meaning if records from all three tables share the same tid"""
    joinByCoor = []
    for loc1 in locList1:
        for loc2 in locList2:
            for loc3 in locList3:
                if loc1[-1] == loc2[-1] == loc3[-1]:
                    joinByCoor.append((loc1, loc2, loc3))
    return joinByCoor


def checkInBbox(coor, bbox):
    """check if a coordinates fall within a certain bbox"""
    if coor[0] <= bbox[0] and coor[0] >= bbox[2] and coor[1] <= bbox[1] and coor[1] >= bbox[3]:
        return True
    else:
        return False


def coorToBbox(joinlist):
    """if user location falls in the bounding box of location extracted
    from twitter text or url linked page text"""
    fallWithin = []
    for join in joinlist:
        tid = join[0][3]
        print(tid)
        lat = join[0][1]
        lng = join[0][2]
        userCoor = (lat, lng)
        if join[1][4] is not None:  # when bbox is applicable
            maxLat = join[1][4]
            maxLng = join[1][5]
            minLat = join[1][6]
            minLng = join[1][7]
            bBox = (maxLat, maxLng, minLat, minLng)
        else:
            buffer = 0.15  # unit: degree, define a buffer zone, if no direct bbox
            maxLat = join[1][2] + buffer
            maxLng = join[1][3] + buffer
            minLat = join[1][2] - buffer
            minLng = join[1][3] - buffer
            bBox = (maxLat, maxLng, minLat, minLng)

        if checkInBbox(userCoor, bBox):
            fallWithin.append((tid, userCoor, bBox))
    return fallWithin


dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
user_coor_tb = "test_usercoor"
text_coor_tb = "test_textcoor"
url_coor_tb = "test_urlcoor"

user_Coors = queryFromDB.get_allData(dbConnect, user_coor_tb)
print("user shared location select finished", len(user_Coors))
text_Coors = queryFromDB.get_allData(dbConnect, text_coor_tb)
print("user twitted location select finished", len(text_Coors))
url_Coors = queryFromDB.get_allData(dbConnect, url_coor_tb)
print("url web page location select finished", len(url_Coors))

user_tw_join = locExtraction_2(user_Coors, text_Coors)
user_tw_within = coorToBbox(user_tw_join)
print(user_tw_within)