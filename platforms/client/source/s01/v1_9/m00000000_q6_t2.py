import mysql.connector
import pandas as pd

config = {
  'user': 'root',
  'password': 'my-secret-pw',
  'host': 'mysql',
  'database': 'tpch',
  'raise_on_warnings': True
}

cnx = mysql.connector.connect(**config)

query = '''
SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE
FROM lineitem
WHERE L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE < '1995-01-01'
AND L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01 AND L_QUANTITY < 24;
'''

try:
    dataframe = pd.read_sql(query, cnx)
    dataframe.to_csv('query_output.csv', index=False)
finally:
    cnx.close()
