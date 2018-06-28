"""
Created on 6/28/2018
@author: Jingchao Yang
"""
import re


def localGazetter(textList):
    """
    Extract local gazetteers from text (twitters or url linked pages)
    :param textList: text list with twitter id
    :return: extracted location name list
    """
    roadDesc = ['road', 'rd', 'street', 'st', 'drive', 'dr', 'suqare', 'sq', 'fm', 'blvd']
    road_extracts = []
    for text in textList:
        if text[1] is not None:
            print(text[-1])

            '''Check possible road names'''
            twText = re.sub(r'[^\w]', ' ', text[1])
            twText = twText.split()
            road_nos = [str(s) for s in twText if s.isdigit()]
            road_descs = [str(s) for s in twText if s.lower() in roadDesc]

            if len(road_descs) > 0:
                road_extract = []
                for road_desc in road_descs:
                    road = road_desc
                    ind = twText.index(road_desc)
                    one_word_ahead = str(twText[ind - 1])
                    if one_word_ahead[0].isupper():  # if start with capital latter, more likely to be street name
                        two_word_ahead = str(twText[ind - 2])
                        if two_word_ahead[0].isupper():  # two-word street name are also common
                            road = (two_word_ahead + ' ' + one_word_ahead + ' ' + road_desc)
                            road_extract.append(road)
                        else:  # stick with one-word name is two-word name is not applicable
                            road = (one_word_ahead + ' ' + road_desc)
                            road_extract.append(road)

                    if len(road_nos) > 0:  # attach road No. with road name is applicable
                        for road_no in road_nos:
                            road_extract.append(road_no + ' ' + road)
                road_extracts.append((road_extract, text[-1]))

            '''Check possible gazetteer names'''

    return road_extracts
