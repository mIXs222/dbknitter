from pymongo import MongoClient
client = MongoClient('localhost', 27017)
db = client['fiction']
db = client.fiction
table = db.people
table.insert_many([
    {"name": "Vincent Vega", "age": 38},
    {"name": "Jules", "age": 41},
    {"name": "Mia", "age": 34},
    {"name": "Marsellus", "age": 45},
    {"name": "Mr. Wolf", "age": 56},
])
print(f'Ingested {table.count_documents({})} rows')
