import csv
import pymysql
import pymongo
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Function to perform MySQL query
def mysql_query(query):
    mysql_cursor.execute(query)
    return mysql_cursor.fetchall()

# Fetch required data from MySQL
mysql_data = mysql_query("""
    SELECT
        O_ORDERKEY,
        O_CUSTKEY,
        O_ORDERSTATUS,
        O_TOTALPRICE,
        O_ORDERDATE,
        O_ORDERPRIORITY,
        O_CLERK,
        O_SHIPPRIORITY,
        O_COMMENT
    FROM orders
""")

# Fetch required data from MongoDB collections
part = list(mongodb.part.find({"P_NAME": {"$regex": ".*dim.*"}}, {"P_PARTKEY": 1}))
supplier = list(mongodb.supplier.find({}, {"S_SUPPKEY": 1, "S_NATIONKEY": 1}))
nation = list(mongodb.nation.find({}, {"N_NATIONKEY": 1, "N_NAME": 1}))

part_keys = {p["P_PARTKEY"] for p in part}
supplier_dict = {s["S_SUPPKEY"]: s["S_NATIONKEY"] for s in supplier}
nation_dict = {n["N_NATIONKEY"]: n["N_NAME"] for n in nation}

# Combine data with cross-database logic
profits = []
for row in mysql_data:
    o_orderkey = row[0]
    o_orderdate = row[4]
    o_year = datetime.strptime(o_orderdate, '%Y-%m-%d').strftime('%Y')

    lineitem_query = f"""
        SELECT L_EXTENDEDPRICE, L_DISCOUNT, L_QUANTITY, L_PARTKEY, L_SUPPKEY
        FROM lineitem
        WHERE L_ORDERKEY = {o_orderkey}
    """
    lineitems = mysql_query(lineitem_query)

    for item in lineitems:
        l_extendedprice, l_discount, l_quantity, l_partkey, l_suppkey = item
        if l_partkey in part_keys and l_suppkey in supplier_dict:
            s_nationkey = supplier_dict[l_suppkey]
            if s_nationkey in nation_dict:
                n_name = nation_dict[s_nationkey]
                amount = l_extendedprice * (1 - l_discount)

                # Find matching partsupp entity to subtract PS_SUPPLYCOST * L_QUANTITY
                partsupp_query = f"""
                    SELECT PS_SUPPLYCOST
                    FROM partsupp
                    WHERE PS_SUPPKEY = {l_suppkey} AND PS_PARTKEY = {l_partkey}
                """
                try:
                    ps_supplycost = mysql_query(partsupp_query)[0][0]
                    amount -= ps_supplycost * l_quantity
                    profits.append((n_name, o_year, amount))
                except IndexError:
                    continue  # Skip if no matching partsupp found

# Group by NATION and O_YEAR and aggregate SUM_PROFIT
profit_dict = {}
for n_name, o_year, amount in profits:
    if (n_name, o_year) not in profit_dict:
        profit_dict[(n_name, o_year)] = 0
    profit_dict[(n_name, o_year)] += amount

# Sort the results
sorted_profits = sorted(profit_dict.items(), key=lambda k: (k[0][0], -int(k[0][1])))

# Write to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['NATION', 'O_YEAR', 'SUM_PROFIT'])
    for key, sum_profit in sorted_profits:
        writer.writerow([key[0], key[1], round(sum_profit, 2)])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
