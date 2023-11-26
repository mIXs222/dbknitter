import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Fetch data from MySQL
with mysql_conn.cursor() as mysql_cursor:
    mysql_cursor.execute(
        "SELECT PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST "
        "FROM partsupp "
        "WHERE PS_SUPPLYCOST = ("
        "SELECT MIN(PS_SUPPLYCOST) "
        "FROM partsupp, supplier, nation, region "
        "WHERE S_SUPPKEY = PS_SUPPKEY "
        "AND S_NATIONKEY = N_NATIONKEY "
        "AND N_REGIONKEY = R_REGIONKEY "
        "AND R_NAME = 'EUROPE')"
    )
    
    partsupp_records = mysql_cursor.fetchall()

# Fetch parts data from MongoDB
parts_query = {'P_SIZE': 15, 'P_TYPE': {'$regex': 'BRASS'}}
parts = mongo_db.part.find(parts_query, {'_id': 0, 'P_PARTKEY': 1, 'P_MFGR': 1})

# Hashmap to store valid part keys and attributes
valid_parts = {part['P_PARTKEY']: part['P_MFGR'] for part in parts}

# Query for suppliers and nations from MongoDB
suppliers_cursor = mongo_db.supplier.aggregate([
    {
        '$lookup': {
            'from': 'nation',
            'localField': 'S_NATIONKEY',
            'foreignField': 'N_NATIONKEY',
            'as': 'nation'
        }
    },
    {'$unwind': '$nation'},
    {
        '$match': {
            'nation.N_REGIONKEY': mongo_db.region.find_one({'R_NAME': 'EUROPE'})['R_REGIONKEY']
        }
    },
    {
        '$project': {
            '_id': 0,
            'S_SUPPKEY': 1,
            'S_ACCTBAL': 1,
            'S_NAME': 1,
            'S_ADDRESS': 1,
            'S_PHONE': 1,
            'S_COMMENT': 1,
            'nation.N_NAME': 1
        }
    },
    {'$sort': {'S_ACCTBAL': -1, 'nation.N_NAME': 1, 'S_NAME': 1}}
])

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT'])

    # Go through the partsupp records and match against MongoDB results
    for ps_record in partsupp_records:
        partkey, suppkey, supplycost = ps_record
        if partkey in valid_parts:
            for supplier in suppliers_cursor:
                if supplier['S_SUPPKEY'] == suppkey:
                    csvwriter.writerow([
                        supplier['S_ACCTBAL'],
                        supplier['S_NAME'],
                        supplier['nation']['N_NAME'],
                        partkey,
                        valid_parts[partkey],
                        supplier['S_ADDRESS'],
                        supplier['S_PHONE'],
                        supplier['S_COMMENT']
                    ])
                    break  # Break since we found the matching supplier for the partsupp record

# Close connections
mysql_conn.close()
mongo_client.close()
