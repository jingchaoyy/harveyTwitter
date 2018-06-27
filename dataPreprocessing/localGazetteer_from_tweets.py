"""
Created on 6/25/2018
@author: Jingchao Yang

1. crowdsource: https://www.chron.com/news/houston-weather/hurricaneharvey/article/These-are-the-roads-that-are-closed-in-Houston-12003482.php
2. USGS: https://stn.wim.usgs.gov/fev/#HarveyAug2017

Goal: Comparing with location (city name) extracted from text, this experiment using local gazetteer information,
providing more more local, and more specific analysis
"""
import pandas as pd

allData = pd.read_csv('C:\\Users\\no281\\Documents\\harVeyTwitter\\FilteredHWMs.csv')


def qualityControl(textList, filterList, qualityList):
    """filter out poor quality record from the original csv file"""
    # textList: list of original text
    # filterList: list of qualities meet requirements
    # qualityList: list of qualities associated to text list
    qualifiedTextList = []
    for i in range(len(qualityList)):
        if any(e in qualityList[i] for e in filterList):
            qualifiedText = textList[i]
            qualifiedTextList.append(qualifiedText)
    return qualifiedTextList

def Remove(locList):
    final_list = []
    for loc in locList:
        if loc not in final_list:
            final_list.append(loc)

    return final_list

qualityFilter = ['Excellent', 'Good', 'Fair']
hwmQualityName = allData['hwmQualityName'].values.tolist()
siteDescription = allData['siteDescription'].values.tolist()  # all location that marked as High Water

qualified_loc = qualityControl(siteDescription, qualityFilter, hwmQualityName)
qualified_loc = Remove(qualified_loc)
print(len(qualified_loc))
for ql in qualified_loc:
    print(ql)
