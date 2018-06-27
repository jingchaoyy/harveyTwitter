"""
Created on 6/14/2018
@author: Jingchao Yang
"""
from psqlOperations import queryFromDB
import re
import enchant

# def singleColumn_nonFilter(dbConnect, tabName, cloName):
#     data = queryFromDB.get_colData(dbConnect, tabName, cloName)
#     rows = []
#     for row in data:
#         row = row.strip()
#         rows.append(row)
#
#     return rows

def singleColumn_wFilter(dbConnect, tabName, cloName):
    data = queryFromDB.get_colData(dbConnect, tabName, cloName)
    # print('Total tweets:', data)
    rows = []
    checkEng = enchant.Dict("en_US")
    for row in data:
        text = row[1]
        text = text.split('https:')[0]
        text = queryFromDB.remove_emoji(text)  # remove emoji
        text = re.sub(r'[^\w]', ' ', text)  # symbol filter
        text = re.sub(r'\s+', ' ', text)  # remove extra space between words
        text = text.strip()

        words = text.split(' ')
        totalWords = len(words)

        engCount = 0
        for word in words:
            if word != '':
                if checkEng.check(word):
                    engCount += 1
        # print(engCount / totalWords)
        if engCount / totalWords >= 0.6:  # consider as useful tweets if 60% or more character are English
            # print(str(row[0]) + ', ' + row[1])
            rows.append((row[0], row[1]))

    print('English tweets:', len(rows))
    return rows