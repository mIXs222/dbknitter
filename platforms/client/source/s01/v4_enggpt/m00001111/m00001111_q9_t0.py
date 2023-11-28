# PerformAnalysis.py
import pymongo
import pymysql
import csv
from datetime import datetime

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Query MySQL for part and nation data containing 'dim'
with mysql_connection.cursor() as mysql_cursor:
    mysql_cursor.execute("""
        SELECT p.P_PARTKEY, p.P_NAME, n.N_NATIONKEY, n.N_NAME 
        FROM part p 
        JOIN nation n ON p.P_PARTKEY=n.N_NATIONKEY 
        WHERE p.P_NAME LIKE '%dim%'
    """)
    part_nation_data = {row[0]: {'part_name': row[1], 'nation_key': row[2], 'nation_name': row[3]} for row in mysql_cursor.fetchall()}

# Query MongoDB for lineitem and partsupp data
lineitem_data = mongo_db.lineitem.find({
    'L_PARTKEY': {'$in': list(part_nation_data.keys())}
})
partsupp_data = mongo_db.partsupp.find()

# Create a dictionary for partsupp using composite key (PS_PARTKEY, PS_SUPPKEY)
partsupp_dict = {(ps['PS_PARTKEY'], ps['PS_SUPPKEY']): ps for ps in partsupp_data}

# Combine the data
profit_data = []
for lineitem in lineitem_data:
    part_key = lineitem['L_PARTKEY']
    supp_key = lineitem['L_SUPPKEY']
    if (part_key, supp_key) in partsupp_dict:
        partsupp = partsupp_dict[(part_key, supp_key)]
        nation_key = part_nation_data[part_key]['nation_key']
        nation_name = part_nation_data[part_key]['nation_name']
        profit = (lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])) - (partsupp['PS_SUPPLYCOST'] * lineitem['L_QUANTITY'])
        order_year = datetime.strptime(lineitem['L_SHIPDATE'], '%Y-%m-%d %H:%M:%S').year
        profit_data.append((nation_name, order_year, profit))

# Sort the data
sorted_profit_data = sorted(profit_data, key=lambda x: (x[0], -x[1]))

# Write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['Nation', 'Year', 'Profit'])
    csvwriter.writerows(sorted_profit_data)

# Close connections
mysql_connection.close()
mongo_client.close()
