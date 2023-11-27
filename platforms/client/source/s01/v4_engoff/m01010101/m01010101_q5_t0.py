import pymysql
import pymongo
import pandas as pd
from datetime import datetime

def get_mysql_connection(db_name, user, password, host):
    return pymysql.connect(host=host, user=user, passwd=password, db=db_name)

def get_mongo_connection(db_name, host, port):
    client = pymongo.MongoClient(host, port)
    return client[db_name]

def execute_query():
    # Connect to the MySQL server
    mysql_conn = get_mysql_connection('tpch', 'root', 'my-secret-pw', 'mysql')
    mysql_cur = mysql_conn.cursor()

    # Connect to the MongoDB server
    mongo_conn = get_mongo_connection('tpch', 'mongodb', 27017)

    # Fetch nations that are in the ASIA region
    asia_nations = list(mongo_conn.region.find({"R_NAME": "ASIA"}))
    asia_nation_keys = [n["R_REGIONKEY"] for n in asia_nations]

    # Fetch nation keys in the ASIA region
    nations_cursor = mysql_cur.execute('SELECT N_NATIONKEY FROM nation WHERE N_REGIONKEY IN  (%s)' % ','.join(['%s'] * len(asia_nation_keys)), asia_nation_keys)
    nations = mysql_cur.fetchall()
    asia_nation_keys = [n[0] for n in nations]

    # Get customers from ASIA
    asia_customers = list(mongo_conn.customer.find({"C_NATIONKEY": {"$in": asia_nation_keys}}))
    asia_cust_keys = [customer["C_CUSTKEY"] for customer in asia_customers]

    # Get suppliers from ASIA
    asia_suppliers = list(mongo_conn.supplier.find({"S_NATIONKEY": {"$in": asia_nation_keys}}))
    asia_supp_keys = [supplier["S_SUPPKEY"] for supplier in asia_suppliers]

    # Get orders with lineitem transactions where customer and suppliers are in ASIA
    start_date = datetime(1990, 1, 1)
    end_date = datetime(1995, 1, 1)
    qualifying_lineitems = list(mongo_conn.lineitem.aggregate([
        {
            "$match": {
                "L_ORDERKEY": {"$in": asia_cust_keys},
                "L_SUPPKEY": {"$in": asia_supp_keys},
                "L_SHIPDATE": {"$gte": start_date, "$lt": end_date}
            }
        },
        {
            "$group": {
                "_id": "$L_SUPPKEY",
                "revenue": {
                    "$sum": {
                        "$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]
                    }
                }
            }
        },
        {"$sort": {"revenue": -1}}
    ]))

    # Construct the results dataframe
    results = pd.DataFrame(qualifying_lineitems)
    results = results.rename(columns={"_id": "nation_key", "revenue": "revenue_volume"})

    # Get nation names
    nation_dict = {n["N_NATIONKEY"]: n["N_NAME"] for n in mongo_conn.nation.find()}
    results["nation"] = results["nation_key"].apply(lambda nk: nation_dict.get(nk, "Unknown"))

    # Drop the nation_key
    results.drop('nation_key', axis=1, inplace=True)

    # Write to CSV
    results.to_csv('query_output.csv', index=False)
    
    # Close database connections
    mysql_cur.close()
    mysql_conn.close()

if __name__ == "__main__":
    execute_query()
