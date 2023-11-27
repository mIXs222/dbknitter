import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw',
                             database='tpch', charset='utf8mb4')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
nation_collection = mongo_db['nation']

# Get nation data from MongoDB
nation_data = {}
for nation in nation_collection.find():
    nation_data[nation['N_NATIONKEY']] = nation['N_NAME']

# SQL Query for MySQL execution
mysql_query = """
SELECT
    C_CUSTKEY,
    C_NAME,
    O_ORDERKEY,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
    C_ACCTBAL,
    C_ADDRESS,
    C_PHONE,
    C_NATIONKEY,
    C_COMMENT
FROM
    customer,
    orders,
    lineitem
WHERE
    C_CUSTKEY = O_CUSTKEY
    AND L_ORDERKEY = O_ORDERKEY
    AND O_ORDERDATE >= '1993-10-01'
    AND O_ORDERDATE < '1994-01-01'
    AND L_RETURNFLAG = 'R'
GROUP BY
    C_CUSTKEY,
    C_NAME,
    C_ACCTBAL,
    C_PHONE,
    O_ORDERKEY,
    C_ADDRESS,
    C_COMMENT
"""

try:
    # Execute MySQL Query
    mysql_cursor.execute(mysql_query)
    result_set = mysql_cursor.fetchall()

    # Prepare CSV file
    with open('query_output.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        
        # Write CSV Header
        writer.writerow([
            "C_CUSTKEY", "C_NAME", "REVENUE", "C_ACCTBAL", "N_NAME",
            "C_ADDRESS", "C_PHONE", "C_COMMENT"
        ])
        
        # Write data rows
        for row in result_set:
            # Append N_NAME from MongoDB nation_data by C_NATIONKEY
            n_name = nation_data.get(row[7]) 
            if n_name:
                writer.writerow(row[:-1] + (n_name,))
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    mysql_cursor.close()
    mysql_conn.close()
    mongo_client.close()
