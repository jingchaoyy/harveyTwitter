"""
Created on 6/20/2018
@author: Jingchao Yang
"""
import geograpy


def urlFilter(urlList, filterList):
    filteredURL = []
    for url in urlList:
        if url[1] is not None and not any(e in url[1] for e in filterList):
            filteredURL.append(url)
    return filteredURL


def findLocFromURL(urlList):
    findLoc = []
    for url in urlList:
        places = geograpy.get_place_context(url=url[1])
        addr = places.address_strings
        print(addr)
        if len(addr) > 0:
            findLoc.append((url[0], addr))
    return findLoc
