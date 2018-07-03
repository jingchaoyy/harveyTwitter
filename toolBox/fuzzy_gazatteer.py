"""
Created on 6/28/2018
@author: Jingchao Yang
"""
import re


def localGazetter(textList):
    """
    Extract local gazetteers from text (twitters or url linked pages)
    :param textList: text list with twitter id
    :return: extracted location name lists (one for roads, one for places)
    """
    roadDesc = ['road', 'rd', 'street', 'st', 'drive', 'dr', 'square', 'sq', 'fm', 'blvd', 'hwy', 'highway', 'avenue',
                'ave']
    placeDesc = ['church', 'school', 'center', 'campus', 'university', 'library', 'station', 'hospital']
    road_extracts, place_extracts = [], []
    for text in textList:  # (text[0]: events, text[1]: text, text[-1]: tw id)
        if text[1] is not None:
            print(text[-1])

            twText = re.sub(r'[^\w]', ' ', text[1])
            twText = twText.split()

            '''Check possible road names
            Road name rules: Commonly, there are roads with one-word name (e.g. Main Street); one-word name and a
            road No. ahead (e.g. 9922 Fairfax Sq), two-word name, two-word name and a road No. ahead, or simply 
            road/street/hwy and a road No. behind. Road names are usually start with capital letters, crucial to our
            name extraction algorithm.
            These common rules are widely used, and will be examed below. More rules may added when project goes
            '''

            road_extract, road_descs = [], []
            p = re.compile('I+\d')  # regular expression, for extracting road name like I45
            for twt in twText:
                if p.match(twt):
                    road_extract.append(twt)
                if twt.lower() in roadDesc:
                    road_descs.append(str(twt))

            # road_nos = [str(s) for s in twText if s.isdigit()]
            # road_descs = [str(s) for s in twText if s.lower() in roadDesc]
            if len(road_descs) > 0:
                for road_desc in road_descs:
                    road = road_desc
                    ind = twText.index(road_desc)
                    one_word_ahead = str(twText[ind - 1])
                    if one_word_ahead[0].isupper():  # if start with capital latter, more likely to be street name
                        two_word_ahead = str(twText[ind - 2])
                        if two_word_ahead[0].isupper():  # two-word street name are also common
                            three_word_ahead = str(twText[ind - 3])
                            if three_word_ahead.isdigit():  # sometime a No. ahead, but not a three-word street name
                                road = (three_word_ahead + ' ' + two_word_ahead + ' '
                                        + one_word_ahead + ' ' + road_desc)
                                road_extract.append(road)
                            else:  # maybe a two-word name without a road No.
                                road = (two_word_ahead + ' ' + one_word_ahead + ' ' + road_desc)
                                road_extract.append(road)
                        else:
                            if two_word_ahead.isdigit():  # or maybe one-word name with a road No.
                                road = (two_word_ahead + ' ' + one_word_ahead + ' ' + road_desc)
                                road_extract.append(road)
                            else:  # stick with one-word name if two-word name is not applicable
                                road = (one_word_ahead + ' ' + road_desc)
                                road_extract.append(road)
                    else:  # name with only a No. (ahead or behind)
                        if one_word_ahead.isdigit():
                            road = (one_word_ahead + ' ' + road_desc)
                            road_extract.append(road)
                        elif len(twText) > ind + 1:  # if there are any string behind the keyword
                            one_word_behind = str(twText[ind + 1])
                            if one_word_behind.isdigit():
                                road = (road_desc + ' ' + one_word_behind)
                                road_extract.append(road)

                    # if len(road_nos) > 0:  # attach road No. with road name is applicable
                    #     for road_no in road_nos:
                    #         road_extract.append(road_no + ' ' + road)

                if len(road_extract) > 0:
                    road_extracts.append((road_extract, text[-1]))

            '''Check possible place names
            Place name rules: Different from road name rules, we ignore the numbers, as usually numbers are not part
            of place naming system. But three-word name for place are popular as well, included this possibility below
            '''
            place_descs = [str(s) for s in twText if s.lower() in placeDesc]
            if len(place_descs) > 0:
                place_extract = []
                for place_desc in place_descs:
                    place = place_desc
                    ind = twText.index(place_desc)
                    one_word_ahead = str(twText[ind - 1])
                    if one_word_ahead[0].isupper():  # if start with capital latter, more likely to be street name
                        two_word_ahead = str(twText[ind - 2])
                        if two_word_ahead[0].isupper():
                            three_word_ahead = str(twText[ind - 3])
                            if three_word_ahead[0].isupper():  # three-word place name are also common
                                place = (
                                        three_word_ahead + ' ' + two_word_ahead + ' ' + one_word_ahead + ' ' + place_desc)
                                place_extract.append(place)
                            else:
                                place = (two_word_ahead + ' ' + one_word_ahead + ' ' + place_desc)
                                place_extract.append(place)
                        else:  # stick with one-word name if two-word name is not applicable
                            place = (one_word_ahead + ' ' + place_desc)
                            place_extract.append(place)

                if len(place_extract) > 0:
                    place_extracts.append((place_extract, text[-1]))

    return road_extracts, place_extracts
