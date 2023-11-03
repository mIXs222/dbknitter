import mysql.connector
import csv

db = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

cursor = db.cursor()

sql = """
SELECT
    PS_PARTKEY,
    SUM(PS_SUPPLYCOST * PS_AVAILQTY) AS VALUE
FROM
    partsupp,
    supplier,
    nation
WHERE
    PS_SUPPKEY = S_SUPPKEY
    AND S_NATIONKEY = N_NATIONKEY
    AND N_NAME = 'GERMANY'
GROUP BY
    PS_PARTKEY HAVING
    SUM(PS_SUPPLYCOST * PS_AVAILQTY) >
    (
    SELECT
        SUM(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0001000000
    FROM
        partsupp,
        supplier,
        nation
    WHERE
        PS_SUPPKEY = S_SUPPKEY
        AND S_NATIONKEY = N_NATIONKEY
        AND N_NAME = 'GERMANY'
    )
ORDER BY
    VALUE DESC
"""

cursor.execute(sql)

result = cursor.fetchall()

header = ['PS_PARTKEY', 'VALUE']
with open('query_output.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(header)
    for row in result:
        writer.writerow(row)
