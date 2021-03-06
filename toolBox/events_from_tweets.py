"""
Created on 6/19/2018
@author: Jingchao Yang
"""
from goose3 import Goose
from interruptingcow import timeout
import time


def remove(list):
    """
    Remove duplicate location name. Same location under same resource with same tid should give only one credit,
    instead of multiple

    :param list: location list
    :return: nun-duplicate location list
    """
    removed = []
    for i in list:
        if i not in removed:
            removed.append(i)
    return removed


def eventBack(twList, eList):
    """
    Extract events from text input, back with events and tid
    :param twList: list of text (tweets, of url link pages)
    :param eList: list of events should be looked for in the text
    :return: twitter id with found events
    """
    # twList: original tweet list
    # eList: list of events
    print('start event extraction from text')
    twWithEvents = []
    for tw in twList:
        print(tw[0])
        if len(tw) > 0:
            try:
                for event in eList:
                    if event in tw[1].lower():
                        print(event)
                        twWithEvents.append((tw[0], event))
            except:
                print('event extraction from text failed for', tw[0])
    return twWithEvents


def textExtractor(urlList):
    """
    Extract texts from tweets urls, back with tid with extracted text list
    :param urlList: filtered url list
    :return: a list contain twitter ID with all text extracted from url links
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


def textExtractor_single(url):
    """
    Extract texts from single tweets url, back with tid with extracted text list

    :param url: single url
    :return: a tuple contains twitter ID its text extracted from url link
    """
    # urlList: list of urls with tid
    print('start text extraction from url')
    g = Goose()
    print(url[0])
    text = ''
    try:  # 10 min timeout, in case url not working properly or taking too long
        article = g.extract(url=url[1])
        text = article.cleaned_text
        # with open(
        #         r"C:\\Users\\no281\\Documents\\harVeyTwitter\\articalExtracted\\test\\" + str(
        #             url[0]) + ".txt", 'w') as outfile:
        #     outfile.write(text)
        # outfile.close()
    except:
        print('url break, continue')

    return (url[0], text)
