
# File: query_script.py 

import pymysql
import csv
from datetime import datetime

connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

cursor = connection.cursor()

query = """
    SELECT
        N_NAME AS NATION,
        DATE_FORMAT(O_ORDERDATE, '%%Y') AS O_YEAR,
        L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY AS AMOUNT
    FROM
        part,
        supplier,
        lineitem,
        partsupp,
        orders,
        nation
    WHERE
        S_SUPPKEY = L_SUPPKEY
        AND PS_SUPPKEY = L_SUPPKEY
        AND PS_PARTKEY = L_PARTKEY
        AND P_PARTKEY = L_PARTKEY
        AND O_ORDERKEY = L_ORDERKEY
        AND S_NATIONKEY = N_NATIONKEY
        AND P_NAME LIKE '%%dim%%'
"""

cursor.execute(query)

result = cursor.fetchall()

profit_dict = {}
for row in result:
    nation = row[0]
    year = row[1]
    profit = row[2]

    if nation not in profit_dict:
        profit_dict[nation] = {}
    if year not in profit_dict[nation]:
        profit_dict[nation][year] = 0

    profit_dict[nation][year] += profit

output = []
for nation in sorted(profit_dict.keys()):
    for year in sorted(profit_dict[nation].keys(), reverse=True):
        output.append([nation, year, profit_dict[nation][year]])

with open('query_output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerows(output)
cursor.close()
connection.close()
