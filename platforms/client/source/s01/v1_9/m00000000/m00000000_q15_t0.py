import pymysql
import csv
from contextlib import closing

def execute_query():
    db_config = {
        'user': 'root',
        'password': 'my-secret-pw',
        'host': 'mysql',
        'database': 'tpch'
    }
    
    query = """
    WITH revenue0 AS (
    SELECT L_SUPPKEY AS SUPPLIER_NO,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS TOTAL_REVENUE
    FROM lineitem
    WHERE L_SHIPDATE >= '1996-01-01'
    AND L_SHIPDATE < DATE_ADD('1996-01-01', INTERVAL 3 MONTH)
    GROUP BY L_SUPPKEY)
    SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE, TOTAL_REVENUE
    FROM supplier, revenue0
    WHERE S_SUPPKEY = SUPPLIER_NO
    AND TOTAL_REVENUE = (
    SELECT MAX(TOTAL_REVENUE) FROM revenue0
    )
    ORDER BY S_SUPPKEY
    """
    with closing(pymysql.connect(**db_config)) as conn, conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        
        with open('query_output.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE'])
            writer.writerows(rows)
            
if __name__ == "__main__":
    execute_query()
