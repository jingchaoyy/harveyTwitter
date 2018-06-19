"""
Created on 6/13/2018
@author: Jingchao Yang
"""
from psqlOperations import queryClean
from collections import Counter


def wordCount(twList):
    # twList: list of tweets (tid, text)
    strMerge = ''
    for tw in twList:
        strMerge += ',' + tw[1]
    for char in '-.,\n':
        strMerge = strMerge.replace(char, ' ')
    strMerge = strMerge.lower()
    word_list = strMerge.split()
    common = Counter(word_list).most_common()

    return common


def checkStopwords(wList):
    # wList: list of words to be checked
    stopwords = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself",
                 "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself",
                 "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that",
                 "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had",
                 "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as",
                 "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through",
                 "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off",
                 "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how",
                 "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not",
                 "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should",
                 "now"]
    wListFilter = []
    for w in wList:
        if not any(e in w[0] for e in stopwords):
            wListFilter.append(w)
    return wListFilter


if __name__ == "__main__":
    dbConnect = "dbname='harveyTwitts' user='postgres' host='localhost' password='123456'"
    tabName = "test"
    cloName = "ttext"

    data_text = queryClean.singleColumn_wFilter(dbConnect, tabName, cloName)
    comWords = wordCount(data_text)
    print(comWords)
    afterCheck = checkStopwords(comWords)
    print(afterCheck)
