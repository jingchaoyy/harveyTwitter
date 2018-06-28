"""
Created on 6/20/2018
@author: Jingchao Yang
"""
import geograpy
from goose3 import Goose
from interruptingcow import timeout
import time


def urlFilter(urlList, filterList):
    """
    :param urlList: all urls from twitters
    :param filterList: list of strings that a url may have (these certain urls should be eliminated)
    :return: filtered urls from twitters
    """
    print('start filter url')
    filteredURL = []
    for url in urlList:
        if url[1] is not None and not any(e in url[1] for e in filterList):
            filteredURL.append(url)
    return filteredURL


def findLocFromURL(urlList):
    """
    extract location info directly from a url
    :param urlList: list of filtered urls
    :return: location names
    """
    print('start extract location from url')
    findLoc = []
    for url in urlList:
        print(url[0])
        places = geograpy.get_place_context(url=url[1])
        addr = places.address_strings
        print(addr)
        if len(addr) > 0:
            findLoc.append((url[0], addr))
    return findLoc


def textExtractor(urlList):
    """
    Extract texts from tweets urls, back with tid with extracted text list
    :param urlList: list of filtered urls
    :return: text from url linked page
    """
    # urlList: list of urls with tid
    print('start text extraction from url')
    g = Goose()
    if urlList:
        textList = []
        time_out = time.process_time() + 5

        while time.process_time() <= time_out:
            for url in urlList:
                print(url[0])
                try:  # 10 min timeout, in case url not working properly or taking too long
                    article = g.extract(url=url[1])
                    text = article.cleaned_text
                    textList.append((url[0], text))
                    # with open(
                    #         r"C:\\Users\\no281\\Documents\\harVeyTwitter\\articalExtracted\\test\\" + str(
                    #             url[0]) + ".txt", 'w') as outfile:
                    #     outfile.write(text)
                    # outfile.close()
                except:
                    print('url break, continue')
    return textList
