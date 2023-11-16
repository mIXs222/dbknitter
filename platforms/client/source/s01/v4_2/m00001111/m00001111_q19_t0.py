import pymysql
from pymongo import MongoClient
import pandas as pd

mysql_conn = pymysql.connect(
    user='root',
    passwd='my-secret-pw',
    host='mysql',
    db='tpch',
    charset='utf8'
)

mongodb_client = MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

cursor = mysql_conn.cursor()
cursor.execute("SELECT * FROM part WHERE P_BRAND IN ('Brand#12', 'Brand#23', 'Brand#34') AND \
                P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG', \
                                'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK', \
                                'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND \
                P_SIZE BETWEEN 1 AND 15")

mysql_data = cursor.fetchall()

df_part = pd.DataFrame(mysql_data, columns=['P_PARTKEY', 'P_NAME', 'P_BRAND', \
                                            'P_TYPE', 'P_SIZE', 'P_CONTAINER', 
                                            'P_RETAILPRICE', 'P_COMMENT'])

lineitem_data = list(mongodb_db.lineitem.find({
    "L_PARTKEY": {"$in": df_part['P_PARTKEY'].tolist()},
    "L_QUANTITY": {"$gte": 1, "$lte": 30},
    "L_SHIPMODE": {"$in": ['AIR', 'AIR REG']},
    "L_SHIPINSTRUCT": 'DELIVER IN PERSON'
}))

df_lineitem = pd.DataFrame(lineitem_data)

df_combined = pd.merge(df_part, df_lineitem, left_on='P_PARTKEY', right_on='L_PARTKEY')

df_combined['REVENUE'] = df_combined['L_EXTENDEDPRICE']* (1 - df_combined['L_DISCOUNT'])
output = df_combined['REVENUE'].sum()

output_df = pd.DataFrame([output], columns=["REVENUE"])
output_df.to_csv('query_output.csv', index=False)

mysql_conn.close()
mongodb_client.close()
