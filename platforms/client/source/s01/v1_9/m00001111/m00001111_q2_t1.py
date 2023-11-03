from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient
import pandas as pd

# For mysql
sql_engine = create_engine("mysql+pymysql://root:my-secret-pw@mysql/tpch")
SQLSession = sessionmaker(bind=sql_engine)
sql_session = SQLSession()

# For mongodb
mongo_client = MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
