import pymysql
import pymongo
import csv


# Function to connect to MySQL and execute query
def query_mysql():
    try:
        # Connect to MySQL
        mysql_connection = pymysql.connect(host='mysql',
                                           user='root',
                                           password='my-secret-pw',
                                           database='tpch')
        cursor = mysql_connection.cursor()

        # Query to get European regions, parts of type BRASS and size 15, and their nation keys
        cursor.execute("""SELECT r.R_NAME, p.P_PARTKEY, p.P_MFGR, n.N_NAME, n.N_NATIONKEY
                          FROM region r
                          JOIN nation n ON r.R_REGIONKEY = n.N_REGIONKEY
                          JOIN part p ON p.P_TYPE = 'BRASS' AND p.P_SIZE = 15
                          WHERE r.R_NAME = 'EUROPE'""")
        result = cursor.fetchall()

        # Close connections
        cursor.close()
        mysql_connection.close()

        # Transform result into dictionary for easier manipulation
        return {row[1]: {
            'P_PARTKEY': row[1],
            'P_MFGR': row[2],
            'N_NAME': row[3],
            'N_NATIONKEY': row[4]
        } for row in result}

    except Exception as e:
        print(f"Error querying MySQL: {e}")
        return {}


# Function to connect to MongoDB and execute query
def query_mongodb(parts_dict):
    try:
        # Connect to MongoDB
        mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
        mongo_db = mongo_client['tpch']

        # Collections for supplier and partsupp
        supplier_col = mongo_db['supplier']
        partsupp_col = mongo_db['partsupp']
        
        # Aggregation pipeline to get minimum cost supplier for BRASS parts of size 15 in Europe and their details
        pipeline = [
            {"$match": {"PS_PARTKEY": {"$in": list(parts_dict.keys())}}},
            {"$lookup": {
                "from": "supplier",
                "localField": "PS_SUPPKEY",
                "foreignField": "S_SUPPKEY",
                "as": "supplier_info"
            }},
            {"$unwind": "$supplier_info"},
            {"$match": {"supplier_info.S_NATIONKEY": {"$in": [parts_dict[x]['N_NATIONKEY'] for x in parts_dict]}}},
            {"$sort": {"PS_SUPPLYCOST": 1, "supplier_info.S_ACCTBAL": -1,
                       "supplier_info.S_NATIONKEY": 1, "supplier_info.S_NAME": 1, "PS_PARTKEY": 1}},
            {"$group": {
                "_id": "$PS_PARTKEY",
                "S_ACCTBAL": {"$first": "$supplier_info.S_ACCTBAL"},
                "S_NAME": {"$first": "$supplier_info.S_NAME"},
                "S_ADDRESS": {"$first": "$supplier_info.S_ADDRESS"},
                "S_PHONE": {"$first": "$supplier_info.S_PHONE"},
                "S_COMMENT": {"$first": "$supplier_info.S_COMMENT"},
                "S_NATIONKEY": {"$first": "$supplier_info.S_NATIONKEY"},
                "P_MFGR": {"$first": "$PS_MFGR"}
            }}
        ]
        result = partsupp_col.aggregate(pipeline)

        # Return results as a list of dictionaries
        res_list = []
        for row in result:
            # Ensure each supplier is from a European nation
            if row['_id'] in parts_dict and row['S_NATIONKEY'] == parts_dict[row['_id']]['N_NATIONKEY']:
                res_list.append({
                    "S_ACCTBAL": row['S_ACCTBAL'],
                    "S_NAME": row['S_NAME'],
                    "N_NAME": parts_dict[row['_id']]['N_NAME'],
                    "P_PARTKEY": row['_id'],
                    "P_MFGR": parts_dict[row['_id']]['P_MFGR'],
                    "S_ADDRESS": row['S_ADDRESS'],
                    "S_PHONE": row['S_PHONE'],
                    "S_COMMENT": row['S_COMMENT']
                })

        # Close MongoDB connection
        mongo_client.close()

        return res_list

    except Exception as e:
        print(f"Error querying MongoDB: {e}")
        return []

# Main function to integrate MySQL and MongoDB results and write them to a file
def main():
    # Get the necessary parts from MySQL
    parts_dict = query_mysql()

    # Get the supplier information and minimum cost from MongoDB
    suppliers_list = query_mongodb(parts_dict)

    # Write results to csv file
    with open('query_output.csv', mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["S_ACCTBAL", "S_NAME", "N_NAME", "P_PARTKEY", "P_MFGR", "S_ADDRESS", "S_PHONE", "S_COMMENT"])
        writer.writeheader()
        for supplier in suppliers_list:
            writer.writerow(supplier)

if __name__ == "__main__":
    main()
