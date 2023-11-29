import pymysql
import csv

# MySQL connection setup
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch',
                             charset='utf8mb4')

try:
    with connection.cursor() as cursor:
        # Write the SQL query
        sql_query = """
        SELECT 
            n.N_NAME, 
            p.P_MFGR, 
            p.P_PARTKEY, 
            s.S_ACCTBAL, 
            s.S_ADDRESS, 
            s.S_COMMENT, 
            s.S_NAME, 
            s.S_PHONE
        FROM 
            nation AS n
        JOIN 
            supplier AS s ON s.S_NATIONKEY = n.N_NATIONKEY
        JOIN 
            partsupp AS ps ON ps.PS_SUPPKEY = s.S_SUPPKEY
        JOIN 
            part AS p ON p.P_PARTKEY = ps.PS_PARTKEY
        JOIN 
            region AS r ON r.R_REGIONKEY = n.N_REGIONKEY
        WHERE 
            r.R_NAME = 'EUROPE'
        AND 
            p.P_TYPE = 'BRASS'
        AND 
            p.P_SIZE = 15
        AND 
            ps.PS_SUPPLYCOST = (
                SELECT MIN(ps_inner.PS_SUPPLYCOST)
                FROM partsupp AS ps_inner
                JOIN supplier AS s_inner ON s_inner.S_SUPPKEY = ps_inner.PS_SUPPKEY
                JOIN nation AS n_inner ON n_inner.N_NATIONKEY = s_inner.S_NATIONKEY
                JOIN part AS p_inner ON p_inner.P_PARTKEY = ps_inner.PS_PARTKEY
                JOIN region AS r_inner ON r_inner.R_REGIONKEY = n_inner.N_REGIONKEY
                WHERE 
                    p_inner.P_PARTKEY = p.P_PARTKEY
                AND 
                    r_inner.R_NAME = 'EUROPE'
                AND 
                    p_inner.P_TYPE = 'BRASS'
                AND 
                    p_inner.P_SIZE = 15
            )
        ORDER BY 
            s.S_ACCTBAL DESC, n.N_NAME ASC, s.S_NAME ASC, p.P_PARTKEY ASC;
        """
        cursor.execute(sql_query)
        result = cursor.fetchall()

        # Writing result to CSV
        with open('query_output.csv', 'w', newline='', encoding='utf-8') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write header
            csvwriter.writerow([x[0] for x in cursor.description])
            # Write data
            for row in result:
                csvwriter.writerow(row)

finally:
    connection.close()
