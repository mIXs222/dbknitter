import pandas as pd
import pymysql

# Open database connection
db = pymysql.connect(host='mysql', 
                     user='root', 
                     passwd='my-secret-pw', 
                     db='tpch')

# SQL Query
sql = """
SELECT
    P_BRAND,
    P_TYPE,
    P_SIZE,
    COUNT(DISTINCT PS_SUPPKEY) AS SUPPLIER_CNT
FROM
    partsupp,
    part
WHERE
    P_PARTKEY = PS_PARTKEY
    AND P_BRAND <> 'Brand#45'
    AND P_TYPE NOT LIKE 'MEDIUM POLISHED%'
    AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND PS_SUPPKEY NOT IN (
        SELECT
            S_SUPPKEY
        FROM
            supplier
        WHERE
            S_COMMENT LIKE '%Customer%Complaints%'
    )
GROUP BY
    P_BRAND,
    P_TYPE,
    P_SIZE
ORDER BY
    SUPPLIER_CNT DESC,
    P_BRAND,
    P_TYPE,
    P_SIZE
"""

# Execute SQL and fetch the result into pandas DataFrame
df = pd.read_sql(sql, con=db)

# Write result to csv
df.to_csv('query_output.csv', index=False)

# Disconnect from server
db.close()
