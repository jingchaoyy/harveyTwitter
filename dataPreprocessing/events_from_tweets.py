"""
Created on 6/19/2018
@author: Jingchao Yang
"""
from goose3 import Goose


def eventBack(twList, eList):
    """Extract events from text input, back with events and tid"""
    # twList: original tweet list
    # eList: list of events
    print('start event extraction from text')
    twWithEvents = []
    for tw in twList:
        print(tw[0])
        if len(tw) > 0:
            for event in eList:
                if event in tw[1].lower():
                    print(event)
                    twWithEvents.append((tw[0], event))
    return twWithEvents


def textExtractor(urlList):
    """Extract texts from tweets urls, back with tid with extracted text list"""
    # urlList: list of urls with tid
    print('start text extraction from url')
    g = Goose()
    if urlList:
        textList = []
        for url in urlList:
            print(url[0])
            try:
                article = g.extract(url=url[1])
                text = article.cleaned_text
                textList.append((url[0], text))
                # with open(
                #         r"C:\\Users\\no281\\Documents\\harVeyTwitter\\articalExtracted\\test\\" + str(
                #             url[0]) + ".txt", 'w') as outfile:
                #     outfile.write(text)
                # outfile.close()
            except:
                print('url broken, continue')
    return textList
