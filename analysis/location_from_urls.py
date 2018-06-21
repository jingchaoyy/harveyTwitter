"""
Created on 6/20/2018
@author: Jingchao Yang
"""
from toolBox import url_tools
from psqlOperations import queryFromDB
from toolBox import location_tools


def locationFilter(locList, filterList, region):  # filter out location out of certain region
    filteredLoc = []
    for loc in locList:
        if len(loc) > 0:
            for l in loc[1]:
                country = l.split(',')[2]
                if region in country and not any(e in l for e in filterList):
                    print((loc[0], l))
                    filteredLoc.append((loc[0], l))
    return filteredLoc


dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tb_out_Name = "test"
clo_url = "teu_url"
allURLs = queryFromDB.get_colData(dbConnect, tb_out_Name, clo_url)
print('url collecting finished', len(allURLs))

filters = ['twitter.com', 'youtube.com', 'instagram.com']  # remove links that are from social media
filteredURLs = url_tools.urlFilter(allURLs, filters)
print('url filtering finished', len(filteredURLs))
urlToLoc = url_tools.findLocFromURL(filteredURLs)
print('loc to url assign finished', len(urlToLoc))

setCountry = 'United States'  # set a region, all location should reside inside
locFilter = ['Harvey', 'Hurricane', 'Homeland']  # currently not considered as location
filteredLoc = locationFilter(urlToLoc, locFilter, setCountry)
print('loc filtering finished', len(filteredLoc))

loc_nonDup = location_tools.Remove(filteredLoc)
print('remove duplicate loc finished', len(loc_nonDup))

coorFromLoc_nonDup = location_tools.locToCoor(loc_nonDup)
print('coor to loc assign finished', len(coorFromLoc_nonDup))

text_LocCoors = location_tools.coorToTexts(coorFromLoc_nonDup, filteredLoc)
print('correlate coor back to all loc found in url finished', len(coorFromLoc_nonDup))
