"""
Created on 6/13/2018
@author: Jingchao Yang
"""
import psycopg2
import re
import enchant


def get_colData(dbc, tbn, clo):
    """ query data from the vendors table """
    conn = None
    try:
        conn = psycopg2.connect(dbc)
        cur = conn.cursor()
        cur.execute("select " + clo + " from " + tbn)
        print("The number of parts: ", cur.rowcount)
        row = cur.fetchone()

        rList = []
        while row is not None:
            # print(row)
            rList.append(row)
            row = cur.fetchone()

        return rList

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_colData_Eng(dbc, tbn, clo):  # filter for collecting tweets written in English
    """ query data from the vendors table """
    conn = None
    try:
        conn = psycopg2.connect(dbc)
        cur = conn.cursor()
        cur.execute("select " + clo + " from " + tbn)
        print("The number of parts: ", cur.rowcount)
        row = cur.fetchone()

        rList = []
        checkEng = enchant.Dict("en_US")  # check for English words

        while row is not None:
            engCount = 0
            row = re.sub(r':.*$', ":", row[0])  # remove link
            row = row.replace('https:', '')  # remove 'https'
            words = row.split(' ')
            totalWords = len(words)

            for word in words:
                word = re.sub(r'[^\w]', ' ', word)  # remove symbols
                # print(word)
                if word != '':
                    if checkEng.check(word):  # count one if a word from string is English
                        engCount += 1

            if engCount / totalWords >= 0.6:  # consider as useful tweets if 60% or more character are English
                rList.append(row)

            row = cur.fetchone()

        return rList

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
