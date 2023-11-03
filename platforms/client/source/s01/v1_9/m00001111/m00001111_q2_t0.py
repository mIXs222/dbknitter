import mysql.connector
import pymongo
import pandas as pd

db_config = {
    "user": "root",
    "password": "my-secret-pw",
    "host": "mysql",
    "database": "tpch",
}

mysql_connection = mysql.connector.connect(**db_config)
mysql_cursor = mysql_connection.cursor()

# Retrieve MySQL data
mysql_query = '''
SELECT
    S_ACCTBAL,
    S_NAME,
    N_NAME,
    S_ADDRESS,
    S_PHONE,
    S_COMMENT
FROM
    supplier,
    nation,
    region
WHERE
    S_NATIONKEY = N_NATIONKEY
    AND N_REGIONKEY = R_REGIONKEY
    AND R_NAME = 'EUROPE'
'''
mysql_cursor.execute(mysql_query)
mysql_data = mysql_cursor.fetchall()

mysql_df = pd.DataFrame(mysql_data, columns=[desc[0] for desc in mysql_cursor.description])

# Connect to MongoDB and retrieve data
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]

partsupp_collection = mongo_db["partsupp"]
part_collection = mongo_db["part"]

partsupp_data = partsupp_collection.find({
    "PS_SUPPLYCOST": {
        "$eq": partsupp_collection.find({"R_NAME": "EUROPE"}).sort("PS_SUPPLYCOST", pymongo.ASCENDING).limit(1)[0]["PS_SUPPLYCOST"]
    }
})

part_data = part_collection.find({
    "P_SIZE": 15,
    "P_TYPE": {"$regex": ".*BRASS.*"}
})

partsupp_df = pd.DataFrame(list(partsupp_data))
part_df = pd.DataFrame(list(part_data))

# Merge dataframes and select only required columns
merged_df = pd.merge(
    mysql_df, 
    pd.merge(
        partsupp_df[["PS_PARTKEY", "PS_SUPPKEY", "PS_SUPPLYCOST"]], 
        part_df[["P_PARTKEY", "P_MFGR"]],
        left_on="PS_PARTKEY", 
        right_on="P_PARTKEY",
        suffixes=('_partsupp', '_part'),
    ),
    left_on=["S_SUPPKEY"], 
    right_on=["PS_SUPPKEY"]
)

final_df = merged_df[[
    "S_ACCTBAL",
    "S_NAME",
    "N_NAME",
    "P_PARTKEY_part",
    "P_MFGR",
    "S_ADDRESS",
    "S_PHONE",
    "S_COMMENT"
]].sort_values(["S_ACCTBAL", "N_NAME", "S_NAME", "P_PARTKEY_part"], ascending=[False, True, True, True])

# Write dataframe to csv
final_df.to_csv('query_output.csv', index=False)
