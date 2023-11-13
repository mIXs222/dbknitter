#!/usr/bin/env python

import csv
import mysql.connector
from mysql.connector import Error

def execute_query(query):
        connection = mysql.connector.connect(host='mysql',
                                             database='tpch',
                                             user='root',
                                             password='my-secret-pw')

        cursor = connection.cursor()
        cursor.execute(query)

        rows = cursor.fetchall()
            
        return rows

def write_csv(rows):
    with open('query_output.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(rows)

if __name__ == "__main__":
    query = """
    SELECT
        S_NAME,
        S_ADDRESS
    FROM
        supplier,
        nation
    WHERE
        S_SUPPKEY IN (
        SELECT
            PS_SUPPKEY
        FROM
            partsupp
        WHERE
            PS_PARTKEY IN (
            SELECT
                P_PARTKEY
            FROM
                part
            WHERE
            P_NAME LIKE 'forest%'
        )
        AND
        PS_AVAILQTY > (
            SELECT
                0.5 * SUM(L_QUANTITY)
            FROM
                lineitem
            WHERE
                L_PARTKEY = PS_PARTKEY
            AND L_SUPPKEY = PS_SUPPKEY
            AND L_SHIPDATE >= '1994-01-01'
            AND L_SHIPDATE < '1995-01-01'
            )
        )
        AND S_NATIONKEY = N_NATIONKEY
        AND N_NAME = 'CANADA'
    ORDER BY
        S_NAME
    """

    result = execute_query(query)
    write_csv(result)
