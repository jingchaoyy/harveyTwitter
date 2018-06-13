"""
Created on 6/13/2018
@author: Jingchao Yang
"""
import psycopg2


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
