"""
Created on 6/20/2018
@author: Jingchao Yang
"""
from toolBox import location_tools
from psqlOperations import queryClean

dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
tb_out_Name = "original"

############ Location from text
clo_Text = "ttext"
data_text = queryClean.singleColumn_wFilter(dbConnect, tb_out_Name, clo_Text)
# print('Original English Only Tweets', data_text)

setCountry = 'United States'
locFilter = ['Harvey', 'Hurricane']  # Name list that should not be considered as location under certain event
loc_fromText = location_tools.locFromText(setCountry, data_text, locFilter)
# print('All locations extracted', loc_fromText)

loc_nonDup = location_tools.Remove(loc_fromText)
# print('Non duplicate location list', loc_nonDup)

coorFromLoc_nonDup = location_tools.locToCoor(loc_nonDup)
# print('Associated coordinates', coorFromLoc_nonDup)

text_LocCoors = location_tools.coorToTexts(coorFromLoc_nonDup, loc_fromText)
# print(text_LocCoors)
