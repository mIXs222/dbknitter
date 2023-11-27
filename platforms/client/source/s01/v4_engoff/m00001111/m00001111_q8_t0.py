# query.py

import pymysql
import pymongo
import csv

def get_mysql_connection(db_name, user, password, host):
    return pymysql.connect(
        db=db_name,
        user=user,
        password=password,
        host=host
    )

def get_mongo_connection(db_name, host, port):
    client = pymongo.MongoClient(host, port)
    return client[db_name]

def get_regionkey_asia(cursor):
    query = "SELECT R_REGIONKEY FROM region WHERE R_NAME = 'ASIA';"
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result else None

def get_nationkey_india(cursor):
    query = "SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'INDIA';"
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result else None

def get_revenue_by_year(cursor, nationkey_india, regionkey_asia, year):
    query = """
    SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue
    FROM supplier
    JOIN nation ON S_NATIONKEY = nation.N_NATIONKEY
    JOIN lineitem ON S_SUPPKEY = L_SUPPKEY
    JOIN orders ON L_ORDERKEY = O_ORDERKEY
    JOIN part ON L_PARTKEY = P_PARTKEY
    WHERE nation.N_REGIONKEY = %s
    AND P_TYPE = 'SMALL PLATED COPPER'
    AND S_NATIONKEY = %s
    AND YEAR(O_ORDERDATE) = %s
    GROUP BY YEAR(O_ORDERDATE);
    """
    cursor.execute(query, (regionkey_asia, nationkey_india, year))
    result = cursor.fetchone()
    return result[0] if result else 0

def main():
    # MySQL connection
    mysql_connection = get_mysql_connection('tpch', 'root', 'my-secret-pw', 'mysql')
    mysql_cursor = mysql_connection.cursor()
    
    # Get keys for 'INDIA' and 'ASIA'
    regionkey_asia = get_regionkey_asia(mysql_cursor)
    nationkey_india = get_nationkey_india(mysql_cursor)
    
    # MongoDB connection
    mongo_db = get_mongo_connection('tpch', 'mongodb', 27017)
    
    # Calculate revenue by year from MySQL and MongoDB
    revenue_1995 = get_revenue_by_year(mysql_cursor, nationkey_india, regionkey_asia, 1995)
    revenue_1996 = get_revenue_by_year(mysql_cursor, nationkey_india, regionkey_asia, 1996)

    # Write the results to 'query_output.csv'
    with open('query_output.csv', mode='w') as csv_file:
        writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['year', 'revenue'])
        writer.writerow(['1995', revenue_1995])
        writer.writerow(['1996', revenue_1996])

    # Close the cursor and MySQL connection
    mysql_cursor.close()
    mysql_connection.close()
    
if __name__ == "__main__":
    main()
