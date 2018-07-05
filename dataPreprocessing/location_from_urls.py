"""
Created on 6/20/2018
@author: Jingchao Yang
"""
from toolBox import url_tools
from psqlOperations import queryFromDB
from toolBox import location_tools

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tb1_out_Name = "original"
tb2_out_Name = "original_texteng"
tb1_clo_url = "teu_url"
tb1_clo_tid = "tid"
tb2_clo_tid = "tid"
allURLs = queryFromDB.joinQuery(dbConnect, tb1_out_Name, tb2_out_Name, tb1_clo_url, tb1_clo_tid, tb2_clo_tid)
print('url collecting finished', len(allURLs))

filters = ['twitter.com', 'youtube.com', 'instagram.com', 'radio']  # remove links that are from social media
filteredURLs = url_tools.urlFilter(allURLs, filters)
print('url filtering finished', len(filteredURLs))
urlToLoc = url_tools.findLocFromURL(filteredURLs)
print('loc to url assign finished', len(urlToLoc))

setCountry = 'United States'  # set a region, all location should reside inside
locFilter = ['Harvey', 'Hurricane', 'Homeland']  # currently not considered as location
filteredLoc = location_tools.locationFilter(urlToLoc, locFilter, setCountry)
print('loc filtering finished', len(filteredLoc))

loc_nonDup = location_tools.Remove(filteredLoc)
print('remove duplicate loc finished', len(loc_nonDup))

coorFromLoc_nonDup = location_tools.locToCoor(loc_nonDup)
print('coor to loc assign finished', len(coorFromLoc_nonDup))

text_LocCoors = location_tools.coorToTexts(coorFromLoc_nonDup, filteredLoc)
print('correlate coor back to all loc found in url finished', len(coorFromLoc_nonDup))
