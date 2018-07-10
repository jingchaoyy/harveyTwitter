"""
Created on 6/28/2018
@author: Jingchao Yang
"""
import re
import jellyfish


def localGazetter(textList):
    """
    Extract local gazetteers from text (twitters or url linked pages)
    :param textList: text list with twitter id
    :return: extracted location name lists (one for roads, one for places)
    """
    roadDesc = ['road', 'rd', 'street', 'st', 'drive', 'dr', 'square', 'sq', 'fm', 'boulevard', 'blvd', 'highway',
                'hwy', 'avenue', 'ave', 'ln']
    roadDesc2 = ['North', 'South', 'East', 'West']
    placeDesc = ['church', 'school', 'center', 'campus', 'university', 'library', 'station', 'hospital']
    road_extracts, road_extracts2, place_extracts = [], [], []
    roadMark = '*R '  # adding marks to ease distinguish between road and place in later process
    placeMark = '#P '
    for text in textList:  # (text[0]: events, text[1]: text, text[-1]: tw id)
        if text[1] is not None:
            # print(text[-1])

            twText = re.sub(r'[^\w]', ' ', text[1])
            twText = twText.split()

            '''Check possible road names
            Road name rules: Commonly, there are roads with one-word name (e.g. Main Street); one-word name and a
            road No. ahead (e.g. 9922 Fairfax Sq), two-word name, two-word name and a road No. ahead, or simply 
            road/street/hwy and a road No. behind. Road names are usually start with capital letters, crucial to our
            name extraction algorithm.
            These common rules are widely used, and will be examed below. More rules may added when project goes
            '''

            road_extract, road_descs, road_inds = [], [], []  # for collecting most regular formatted road name
            road_extract2, road_descs2, road_inds2 = [], [], []  # for collecting other formatted road name
            place_descs, place_inds = [], []
            p = re.compile('I+\d')  # regular expression, for extracting road name like I45
            for twt in range(len(twText)):
                if p.match(twText[twt]):
                    road_extract.append(twText[twt])
                if twText[twt].lower() in roadDesc:
                    road_descs.append(str(twText[twt]))
                    road_inds.append(twt)
                if twText[twt] in roadDesc2:
                    road_descs2.append(str(twText[twt]))
                    road_inds2.append(twt)
                if twText[twt].lower() in placeDesc:
                    place_descs.append(str(twText[twt]))
                    place_inds.append(twt)

            # road_nos = [str(s) for s in twText if s.isdigit()]
            # road_descs = [str(s) for s in twText if s.lower() in roadDesc]
            if len(road_descs) > 0:
                for road_desc in range(len(road_descs)):
                    road = road_descs[road_desc]
                    ind = road_inds[road_desc]
                    one_word_ahead = str(twText[ind - 1])
                    if one_word_ahead[0].isupper():  # if start with capital latter, more likely to be street name
                        two_word_ahead = str(twText[ind - 2])
                        if two_word_ahead[0].isupper():  # two-word street name are also common
                            three_word_ahead = str(twText[ind - 3])
                            if three_word_ahead.isdigit():  # sometime a No. ahead, but not a three-word street name
                                road = (roadMark + three_word_ahead + ' ' + two_word_ahead + ' '
                                        + one_word_ahead + ' ' + road)
                                road_extract.append(road)
                            else:  # maybe a two-word name without a road No.
                                road = (roadMark + two_word_ahead + ' ' + one_word_ahead + ' ' + road)
                                road_extract.append(road)
                        else:
                            if two_word_ahead.isdigit():  # or maybe one-word name with a road No.
                                road = (roadMark + two_word_ahead + ' ' + one_word_ahead + ' ' + road)
                                road_extract.append(road)
                            else:  # stick with one-word name if two-word name is not applicable
                                road = (roadMark + one_word_ahead + ' ' + road)
                                road_extract.append(road)
                    else:  # name with only a No. (ahead or behind)
                        if one_word_ahead.isdigit():
                            road = (roadMark + one_word_ahead + ' ' + road)
                            road_extract.append(road)
                        elif len(twText) > ind + 1:  # if there are any string behind the keyword
                            one_word_behind = str(twText[ind + 1])
                            if one_word_behind.isdigit():
                                road = (roadMark + road + ' ' + one_word_behind)
                                road_extract.append(road)

                    # if len(road_nos) > 0:  # attach road No. with road name is applicable
                    #     for road_no in road_nos:
                    #         road_extract.append(road_no + ' ' + road)
                if len(road_extract) > 0:
                    road_extracts.append((road_extract, text[-1]))

            ''' Collecting road names like '501 East Hopkins' '''
            if len(road_descs2) > 0:
                for road_desc2 in range(len(road_descs2)):
                    road2 = road_descs2[road_desc2]
                    ind2 = road_inds2[road_desc2]
                    one_word_ahead2 = str(twText[ind2 - 1])
                    one_word_behind2 = str(twText[ind2 + 1])
                    if one_word_ahead2[0].isupper():  # if start with capital latter, more likely to be street name
                        two_word_ahead2 = str(twText[ind2 - 2])
                        if two_word_ahead2[0].isupper() or two_word_ahead2.isdigit():
                            if one_word_behind2[0].isupper():
                                road2 = (roadMark + two_word_ahead2 + ' ' + one_word_ahead2 + ' ' + road2 + ' '
                                         + one_word_behind2)
                                road_extract2.append(road2)
                            else:
                                road2 = (roadMark + two_word_ahead2 + ' ' + one_word_ahead2 + ' ' + road2)
                                road_extract2.append(road2)
                        else:
                            if one_word_behind2[0].isupper():
                                road2 = (roadMark + one_word_ahead2 + ' ' + road2 + ' ' + one_word_behind2)
                                road_extract2.append(road2)
                            else:
                                road2 = (roadMark + one_word_ahead2 + ' ' + road2)
                                road_extract2.append(road2)
                    elif one_word_ahead2.isdigit():
                        if one_word_behind2[0].isupper():
                            road2 = (roadMark + one_word_ahead2 + ' ' + road2 + ' ' + one_word_behind2)
                            road_extract2.append(road2)
                        else:
                            road2 = (roadMark + one_word_ahead2 + ' ' + road2)
                            road_extract2.append(road2)

                if len(road_extract2) > 0:
                    road_extracts2.append((road_extract2, text[-1]))

                road_extracts = road_extracts + road_extracts2

            '''Check possible place names
            Place name rules: Different from road name rules, we ignore the numbers, as usually numbers are not part
            of place naming system. But three-word name for place are popular as well, included this possibility below
            '''
            # place_descs = [str(s) for s in twText if s.lower() in placeDesc]
            if len(place_descs) > 0:
                place_extract = []
                for place_desc in range(len(place_descs)):
                    place = place_descs[place_desc]
                    ind = place_inds[place_desc]
                    one_word_ahead = str(twText[ind - 1])
                    if one_word_ahead[0].isupper():  # if start with capital latter, more likely to be street name
                        two_word_ahead = str(twText[ind - 2])
                        if two_word_ahead[0].isupper():
                            three_word_ahead = str(twText[ind - 3])
                            if three_word_ahead[0].isupper():  # three-word place name are also common
                                place = (placeMark + three_word_ahead + ' ' + two_word_ahead + ' ' + one_word_ahead
                                         + ' ' + place)
                                place_extract.append(place)
                            else:
                                place = (placeMark + two_word_ahead + ' ' + one_word_ahead + ' ' + place)
                                place_extract.append(place)
                        else:  # stick with one-word name if two-word name is not applicable
                            place = (placeMark + one_word_ahead + ' ' + place)
                            place_extract.append(place)

                if len(place_extract) > 0:
                    place_extracts.append((place_extract, text[-1]))

    return road_extracts, place_extracts


def roadNameFormat(roadName):
    """
    Format road name to enhance string match when using jellyfish

    :param roadName: string, original road name
    :return: formatted road name
    """
    searchList = ['north', 'south', 'west', 'east', 'road', 'street', 'drive', 'square', 'boulevard', 'highway',
                  'avenue']  # list for search and to be replaced
    formatList = ['n', 's', 'w', 'e', 'rd', 'st', 'dr', 'sq', 'blvd', 'hwy', 'ave']

    # format road names
    loc11Split = str(roadName).split(' ')
    loc11Format = []
    for j in loc11Split:
        if j.lower() in searchList:
            formated = formatList[searchList.index(j.lower())]
            loc11Format.append(formated)
        else:
            j = j.lower()
            j = re.sub(r'[^\w]', ' ', j)
            j = j.strip()
            loc11Format.append(j)
    roadName = ' '.join(loc11Format)
    return roadName


def fuzzyLocMatch(locList1, locList2):
    """
    Func for tw self-evaluation, see how location extracted from tw are correlated to those in url

    :param locList1: tw extracted local gazetteers
    :param locList2: url extracted local gazetteers
    :return: score with tid (how reliable the tw is based on its linked url)
    """
    scores = []
    for loc1 in locList1:
        score = 0
        for loc2 in locList2:
            if loc1[-1] == loc2[-1]:
                loc1R = roadNameFormat(loc1[0])
                loc2R = roadNameFormat(loc2[0])
                score = jellyfish.jaro_distance(str(loc1R), str(loc2R))
        if score > 0:
            scores.append((round(score, 2), loc1[-1]))
    return scores


def fuzzyLocMatch_wGT(locList1, locList2):
    """
    Fuzzy location match using string comparision with jellyfish, can be applied to tweets extracted local gazetteers
    and ground truth or url extracted local gazetteers and ground truth

    :param locList1: tw extracted local gazetteers
    :param locList2: ground truth
    :return: score with tid (how reliable the tw is based only on either address or place name fuzzy match)
    """
    scores = []
    for loc1 in locList1:
        print(loc1[-1])
        score = 0
        for loc11 in loc1[0]:
            loc11 = roadNameFormat(loc11)

            for loc2 in locList2:
                loc2 = roadNameFormat(loc2)

                s = jellyfish.jaro_distance(str(loc11), str(loc2))
                # print(str(loc11), '###', str(loc2), s)
                score = max(score, s)

        scores.append((round(score, 2), loc1[-1]))
    return scores
