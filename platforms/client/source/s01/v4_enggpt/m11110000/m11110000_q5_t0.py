import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Retrieve region 'ASIA'
asia_region = mongodb.region.find_one({'R_NAME': 'ASIA'})
if asia_region:
    asia_region_key = asia_region['R_REGIONKEY']

    # Retrieve nations in 'ASIA'
    nations = list(mongodb.nation.find({'N_REGIONKEY': asia_region_key}, {'N_NATIONKEY': 1}))

    # Create a list of nation keys for SQL query
    nation_keys_list = [nation['N_NATIONKEY'] for nation in nations]

    # Prepare the SQL query using the list of nation keys
    sql_query = '''
    SELECT
        n.N_NAME,
        SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as revenue
    FROM
        customer AS c
    JOIN orders AS o ON
        c.C_CUSTKEY = o.O_CUSTKEY
    JOIN lineitem AS l ON
        o.O_ORDERKEY = l.L_ORDERKEY
    JOIN nation AS n ON
        c.C_NATIONKEY = n.N_NATIONKEY
    WHERE
        c.C_NATIONKEY IN (%s)
        AND o.O_ORDERDATE >= '1990-01-01'
        AND o.O_ORDERDATE <= '1994-12-31'
    GROUP BY
        n.N_NAME
    ORDER BY
        revenue DESC;
    '''
    
    # Format SQL query with the list of nation keys
    formatted_sql_query = sql_query % ','.join(str(key) for key in nation_keys_list)

    with mysql_conn.cursor() as cursor:
        cursor.execute(formatted_sql_query)
        result = cursor.fetchall()

        # Write to CSV file
        with open('query_output.csv', mode='w', newline='') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(['N_NAME', 'REVENUE'])
            for row in result:
                csv_writer.writerow(row)

# Close connections
mysql_conn.close()
mongo_client.close()
