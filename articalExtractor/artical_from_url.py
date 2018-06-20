"""
Created on 6/20/2018
@author: Jingchao Yang
"""
from goose3 import Goose
from toolBox import url_tools

def textExtractor(urlList):
    g = Goose()
    if urlList:
        for url in urlList:
            article = g.extract(url=url[1])
            text = article.cleaned_text
            with open(
                    r"C:\\Users\\no281\\Documents\\harVeyTwitter\\articalExtracted\\test\\" + str(
                        url[0]) + ".txt", 'w') as outfile:
                outfile.write(text)
            outfile.close()


filters = ['twitter.com', 'youtube.com']
urls = [(1111, 'http://kxan.com/2017/09/12/funeral-wednesday-for-houston-officer-who-died-in-flooding/?utm_medium=social&utm_source=twitter_KXAN_News'),
        (2222, 'https://www.youtube.com/watch?v=4SZxRCmXB4o&feature=share')]

urlAfterFilter = url_tools.urlFilter(urls, filters)
textExtractor(urlAfterFilter)