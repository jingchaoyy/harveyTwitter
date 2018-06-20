"""
Created on 6/20/2018
@author: Jingchao Yang
"""
def locationFilter(locList, filterList, region):
    filteredLoc = []
    for loc in locList:
        country = loc[1].split(',')[2]
        if region in country and not any(e in loc[1] for e in filterList):
            filteredLoc.append(loc)
    return filteredLoc


setCountry = 'United States'
locFilter = ['Harvey', 'Hurricane']
