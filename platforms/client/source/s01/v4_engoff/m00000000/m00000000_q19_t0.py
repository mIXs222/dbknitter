import pymysql
import csv

def get_connection():
    return pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

def execute_query(conn):
    query = """
    SELECT 
        p.P_BRAND, 
        l.L_QUANTITY, 
        l.L_EXTENDEDPRICE, 
        l.L_DISCOUNT, 
        l.L_SHIPMODE
    FROM 
        part p 
    JOIN 
        lineitem l 
    ON 
        p.P_PARTKEY = l.L_PARTKEY
    WHERE 
        (
            p.P_BRAND = 'Brand#12' AND 
            p.P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND 
            l.L_QUANTITY BETWEEN 1 AND 11 AND 
            p.P_SIZE BETWEEN 1 AND 5
        ) 
        OR 
        (
            p.P_BRAND = 'Brand#23' AND 
            p.P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND 
            l.L_QUANTITY BETWEEN 10 AND 20 AND 
            p.P_SIZE BETWEEN 1 AND 10
        ) 
        OR 
        (
            p.P_BRAND = 'Brand#34' AND 
            p.P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND 
            l.L_QUANTITY BETWEEN 20 AND 30 AND 
            p.P_SIZE BETWEEN 1 AND 15
        ) 
        AND 
        l.L_SHIPMODE IN ('AIR', 'AIR REG')
    """

    with open('query_output.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['P_BRAND', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_SHIPMODE'])
        with conn.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                writer.writerow(row)

def main():
    conn = get_connection()
    try:
        execute_query(conn)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
