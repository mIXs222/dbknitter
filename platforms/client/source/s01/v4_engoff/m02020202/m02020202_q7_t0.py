import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Function to load data from MySQL
def load_mysql_data():
    conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
    try:
        with conn.cursor() as cursor:
            query = """
            SELECT 
                n1.N_NAME AS supplier_nation, 
                n2.N_NAME AS customer_nation, 
                YEAR(o.O_ORDERDATE) AS year, 
                SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue
            FROM 
                orders o
                JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
                JOIN supplier s ON s.S_SUPPKEY = l.L_SUPPKEY
                JOIN nation n1 ON s.S_NATIONKEY = n1.N_NATIONKEY
                JOIN customer c ON c.C_CUSTKEY = o.O_CUSTKEY
                JOIN nation n2 ON c.C_NATIONKEY = n2.N_NATIONKEY
            WHERE 
                (
                    (n1.N_NAME = 'INDIA' AND n2.N_NAME = 'JAPAN')
                    OR (n1.N_NAME = 'JAPAN' AND n2.N_NAME = 'INDIA')
                )
                AND o.O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
            GROUP BY supplier_nation, customer_nation, year
            ORDER BY supplier_nation, customer_nation, year
            """
            cursor.execute(query)
            result = cursor.fetchall()
            return pd.DataFrame(result, columns=['supplier_nation', 'customer_nation', 'year', 'revenue'])
    finally:
        conn.close()

# Function to load data from Redis
def load_redis_data():
    redis_conn = DirectRedis(host='redis', port=6379, db=0)
    # Since Redis does not support complex queries, we need to load the
    # data into pandas for further processing. Assuming that 'get' will
    # return a DataFrame. Adjust if this is not the correct behavior.
    return {
        'supplier': redis_conn.get('supplier'),
        'customer': redis_conn.get('customer'),
        'lineitem': redis_conn.get('lineitem')
    }

def main():
    # Load data from MySQL and Redis
    mysql_data = load_mysql_data()
    redis_data = load_redis_data()
    
    # Write combined results to CSV
    query_output = pd.concat([mysql_data, redis_data], ignore_index=True)
    query_output.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
