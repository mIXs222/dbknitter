# python_code.py
import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection setup
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# MongoDB connection setup
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Get parts and partsupp data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT P_PARTKEY, P_NAME FROM part")
    parts = cursor.fetchall()
    cursor.execute("SELECT PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST FROM partsupp")
    partsupp = cursor.fetchall()

# Filtering parts containing 'dim' in their names and creating a dictionary for faster access
parts_with_dim = {p[0]: p[1] for p in parts if 'dim' in p[1].lower()}

# Create a dictionary to match PS_PARTKEY to PS_SUPPLYCOST
supply_cost_dict = {ps[0]: ps[2] for ps in partsupp if ps[0] in parts_with_dim.keys()}

# Get nation data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation ORDER BY N_NAME ASC")
    nations = cursor.fetchall()

# Create a dictionary to match N_NATIONKEY to N_NAME
nation_dict = {n[0]: n[1] for n in nations}

# Get the required data from MongoDB 'supplier' and 'lineitem' collections
suppliers = list(mongodb.supplier.find({}, {"S_SUPPKEY": 1, "S_NATIONKEY": 1}))
lineitems = list(mongodb.lineitem.find({}, {"L_PARTKEY": 1, "L_SUPPKEY": 1, "L_QUANTITY": 1, "L_EXTENDEDPRICE": 1, "L_DISCOUNT": 1, "L_SHIPDATE": 1}))

# Join the necessary data and calculate the profit
profit_dict = {}
for li in lineitems:
    if li['L_PARTKEY'] in parts_with_dim:
        s_nationkey = next((s['S_NATIONKEY'] for s in suppliers if s['S_SUPPKEY'] == li['L_SUPPKEY']), None)
        if s_nationkey is not None:
            profit = (li['L_EXTENDEDPRICE'] * (1 - li['L_DISCOUNT'])) - (supply_cost_dict[li['L_PARTKEY']] * li['L_QUANTITY'])
            nation = nation_dict[s_nationkey]
            year = datetime.strptime(li['L_SHIPDATE'], "%Y-%m-%d").year
            if nation in profit_dict:
                if year in profit_dict[nation]:
                    profit_dict[nation][year] += profit
                else:
                    profit_dict[nation][year] = profit
            else:
                profit_dict[nation] = {year: profit}

# Sorting the result by nation and within each nation by year in descending order
sorted_profit = [(nation, year, profit_dict[nation][year]) for nation in sorted(profit_dict)
                 for year in sorted(profit_dict[nation], reverse=True)]

# Writing the result to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['NATION', 'YEAR', 'PROFIT'])
    for row in sorted_profit:
        writer.writerow(row)

# Close the database connections
mysql_conn.close()
mongo_client.close()
