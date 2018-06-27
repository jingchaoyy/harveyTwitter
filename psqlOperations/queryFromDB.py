"""
Created on 6/13/2018
@author: Jingchao Yang

reference: http://www.postgresqltutorial.com/postgresql-python/query/
"""
import psycopg2
import re
import enchant


def remove_emoji(string):
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', string)


def get_allData(dbc, tbn):
    """ query all data from a table """
    conn = None
    try:
        conn = psycopg2.connect(dbc)
        cur = conn.cursor()
        cur.execute("select * from " + tbn)
        print("The number of parts from table " + tbn, cur.rowcount)
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


def get_colData(dbc, tbn, col):
    """ query data from a table """
    conn = None
    try:
        conn = psycopg2.connect(dbc)
        cur = conn.cursor()
        cur.execute("select tid," + col + " from " + tbn)
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


# def get_colData_Eng(dbc, tbn, clo):  # filter for collecting tweets written in English
#     """ query data from a table """
#     conn = None
#     try:
#         conn = psycopg2.connect(dbc)
#         cur = conn.cursor()
#         cur.execute("select " + clo + " from " + tbn)
#         print("The number of parts: ", cur.rowcount)
#         row = cur.fetchone()
#
#         rList = []
#         checkEng = enchant.Dict("en_US")  # check for English words
#
#         while row is not None:
#             engCount = 0
#             row = re.sub(r'https:.*$', ":", row[0])  # remove link
#             row = remove_emoji(row)
#             words = row.split(' ')
#             totalWords = len(words)
#
#             for word in words:
#                 word = re.sub(r'[^\w]', ' ', word)  # remove symbols
#                 # print(word)
#                 if word != '':
#                     if checkEng.check(word):  # count one if a word from string is English
#                         engCount += 1
#
#             if engCount / totalWords >= 0.6:  # consider as useful tweets if 60% or more character are English
#                 rList.append(row)
#
#             row = cur.fetchone()
#
#         return rList
#
#         cur.close()
#     except (Exception, psycopg2.DatabaseError) as error:
#         print(error)
#     finally:
#         if conn is not None:
#             conn.close()


def get_coorData(dbc, tbn, lat, lng):
    """ query coordinates from a table, two columns together """
    conn = None
    try:
        conn = psycopg2.connect(dbc)
        cur = conn.cursor()
        cur.execute("select tid," + lat + "," + lng + " from " + tbn + " where " + lat + " is not null")
        print("Tweets with coordinates", cur.rowcount)
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


def joinQuery(dbc, tbn1, tbn2, col1, col1_1, col2_1):
    conn = None
    try:
        conn = psycopg2.connect(dbc)
        cur = conn.cursor()
        print("SELECT " + tbn1 + "." + col1 + ", " + tbn2 + "." + col2_1 +
              " FROM " + tbn1 + " INNER JOIN " + tbn2 + " ON " + tbn1 + "." + col1_1 + " = " + tbn2 + "." + col2_1)
        cur.execute(
            "SELECT " + tbn2 + "." + col2_1 + ", " + tbn1 + "." + col1 +
            " FROM " + tbn1 + " INNER JOIN " + tbn2 + " ON " + tbn1 + "." + col1_1 + " = " + tbn2 + "." + col2_1)
        print("The number of parts from table join " + tbn1 + " and " + tbn2, cur.rowcount)
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


def likeQuery(dbc, tbn, col, likeList):
    """
    Query database using like statement to extract events from text

    dbc: database connector
    tbn: table name
    col: column in selected table to be aimed
    likeList: list contain all keywords should be looking for
    """
    conn = None
    try:
        conn = psycopg2.connect(dbc)
        cur = conn.cursor()

        rList = []
        for like in likeList:
            print("START select tid," + col + " from " + tbn + " where " + col + " like '%" + like + "%'")
            cur.execute("select tid," + col + " from " + tbn + " where " + col + " like '%" + like + "%'")
            row = cur.fetchone()

            while row is not None:
                # print(row[0])
                rList.append((row[0] ,like))
                row = cur.fetchone()

        return rList

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
