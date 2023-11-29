import pymysql
import pymongo
import csv
import os
from datetime import datetime

# Connection to MySQL (for tpch MySQL database)
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connection to MongoDB (for tpch MongoDB database)
mongodb_conn = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_conn['tpch']

# Get the set of nationkeys for India and Japan from the nation table in MongoDB
nation_keys = {}
for nation in mongodb_db.nation.find({'N_NAME': {'$in': ['INDIA', 'JAPAN']}}):
    nation_keys[nation['N_NAME']] = nation['N_NATIONKEY']

# Generate a list of supplier keys from the supplier table in MongoDB
supplier_nation_keys = {
    nation: [supplier['S_SUPPKEY'] for supplier in mongodb_db.supplier.find({'S_NATIONKEY': nk})]
    for nation, nk in nation_keys.items()
}

# Get the lineitem and its associated order and customer information
mysql_query = """
    SELECT
        c.C_NATIONKEY, YEAR(o.O_ORDERDATE) as l_year,
        SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as revenue,
        l.L_SUPPKEY
    FROM
        customer as c
    JOIN
        orders as o ON c.C_CUSTKEY = o.O_CUSTKEY
    JOIN
        lineitem as l ON o.O_ORDERKEY = l.L_ORDERKEY
    WHERE
        l.L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
    GROUP BY
        c.C_NATIONKEY, l_year, l.L_SUPPKEY
"""

mysql_cursor.execute(mysql_query)

# Process the result
output_data = []
for row in mysql_cursor.fetchall():
    cust_nation, l_year, revenue, suppkey = row
    cust_nation_name = 'INDIA' if cust_nation == nation_keys['INDIA'] else 'JAPAN'
    # Find supplier nation from the suppkey
    supp_nation_name = next(
        (nation for nation, supp_keys in supplier_nation_keys.items() if suppkey in supp_keys),
        None
    )
    
    # Filter for the required condition: supplier and customer must not be from the same nation
    if supp_nation_name and supp_nation_name != cust_nation_name:
        output_row = {
            'CUST_NATION': cust_nation_name,
            'L_YEAR': l_year,
            'REVENUE': revenue,
            'SUPP_NATION': supp_nation_name
        }
        output_data.append(output_row)

# Sort the output data
sorted_output_data = sorted(output_data, key=lambda x: (x['SUPP_NATION'], x['CUST_NATION'], x['L_YEAR']))

# Write to CSV file
csv_file = 'query_output.csv'
with open(csv_file, 'w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=['CUST_NATION', 'L_YEAR', 'REVENUE', 'SUPP_NATION'])
    writer.writeheader()
    writer.writerows(sorted_output_data)

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongodb_conn.close()
