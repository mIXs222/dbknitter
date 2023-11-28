from pymongo import MongoClient
import pandas as pd
from datetime import datetime

# Function to connect to MongoDB
def connect_to_mongodb(host, port, db_name):
    client = MongoClient(host, port)
    db = client[db_name]
    return db

# Function to perform the analysis
def analyze_lineitems(collection):
    pipeline = [
        {
            '$match': {
                'L_SHIPDATE': {
                    '$lte': datetime(1998, 9, 2)
                }
            }
        },
        {
            '$group': {
                '_id': {'L_RETURNFLAG': '$L_RETURNFLAG', 'L_LINESTATUS': '$L_LINESTATUS'},
                'SUM_QTY': {'$sum': '$L_QUANTITY'},
                'SUM_BASE_PRICE': {'$sum': '$L_EXTENDEDPRICE'},
                'SUM_DISC_PRICE': {
                    '$sum': {
                        '$multiply': [
                            '$L_EXTENDEDPRICE',
                            {'$subtract': [1, '$L_DISCOUNT']}
                        ]
                    }
                },
                'SUM_CHARGE': {
                    '$sum': {
                        '$multiply': [
                            '$L_EXTENDEDPRICE',
                            {'$subtract': [1, '$L_DISCOUNT']},
                            {'$add': [1, '$L_TAX']}
                        ]
                    }
                },
                'AVG_QTY': {'$avg': '$L_QUANTITY'},
                'AVG_PRICE': {'$avg': '$L_EXTENDEDPRICE'},
                'AVG_DISC': {'$avg': '$L_DISCOUNT'},
                'COUNT_ORDER': {'$sum': 1}
            }
        },
        {
            '$sort': {'_id.L_RETURNFLAG': 1, '_id.L_LINESTATUS': 1}
        }
    ]
    return list(collection.aggregate(pipeline))

# Main execution
if __name__ == "__main__":
    db = connect_to_mongodb('mongodb', 27017, 'tpch')
    lineitem_collection = db['lineitem']
    results = analyze_lineitems(lineitem_collection)
    
    # Convert results to a pandas dataframe
    df = pd.DataFrame(results)
    
    # Normalize the '_id' field in the results for output
    df_normalized = pd.json_normalize(df['_id']).join(df.drop('_id', 1))
    df_normalized.rename(columns={'L_RETURNFLAG': 'RETURNFLAG', 'L_LINESTATUS': 'LINESTATUS'}, inplace=True)
    
    # Write output to CSV
    df_normalized.to_csv('query_output.csv', index=False)
