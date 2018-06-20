"""
Created on 6/20/2018
@author: Jingchao Yang
"""
import geograpy
def urlFilter(urlList, filterList):
    filteredURL = []
    for url in urlList:
        if not any(e in url[1] for e in filterList):
            filteredURL.append(url)
    return filteredURL

def findLocFromURL(urlList):
    findLoc = []
    for url in urlList:
        places = geograpy.get_place_context(url=url[1])
        addr = places.address_strings
        findLoc.append((url[0], addr))