"""
Created on 6/7/2018
@author: Jingchao Yang
"""
import json
from itertools import islice


# Recipe from https://docs.python.org/2/library/itertools.html
def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(islice(iterable, n))


if __name__ == "__main__":

    cont = 0
    with open(
            r"C:\Users\/no281\Documents\harVeyTwitter\harvey_twitter_dataset\/02_archive_only\HurricaneHarvey.json") as f:
        while True:
            lines = take(30000, f)
            cont += 1
            if lines:
                print(cont*30000)
                with open(
                        r"C:\Users\/no281\Documents\harVeyTwitter\harvey_twitter_dataset\/02_archive_only\subsets_30000\ss" + str(
                            cont) + ".json", 'w') as outfile:
                    outfile.write(''.join(lines))
                outfile.close()
            else:
                break



####################### test read data
# load each line then read as json
# data = json.load(open(r"C:\Users\no281\Documents\harVeyTwitter\harvey_twitter_dataset\02_archive_only\subsets\ss1.json"))
# test = data['text']
# print(test)
