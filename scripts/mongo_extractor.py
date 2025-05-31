import pandas as pd
from pymongo import MongoClient
import logging
import os
from typing import Optional

def leer_datos_mongo() -> pd.DataFrame:
  client = None

  try:
    host = os.getenv('MONGO_HOST')
    port = int(os.getenv('MONGO_PORT'))
    username = os.getenv('MONGO_USERNAME')
    password = os.getenv('MONGO_PASSWORD')
    database = os.getenv('MONGO_DATABASE')
    collection = 'accounts'

    connection_string = f"mongodb://{username}:{password}@{host}:{port}/"

    logging.info(f"Connecting to MongoDB at {host}:{port}")
    client = MongoClient(connection_string)

    client.admin.command('ping')
    logging.info("MongoDB connection successful")

    db = client[database]
    col = db[collection]

    logging.info(f"Extracting all data from {database}.{collection}")

    cursor = col.find({})
    data = list(cursor)

    if not data:
      logging.warning("No data found in the collection")
      return pd.DataFrame()

    df = pd.DataFrame(data)

    if '_id' in df.columns:
      df = df.drop('_id', axis=1)

    logging.info(f"Successfully extracted {len(df)} records from MongoDB")
    logging.info(f"Columns: {list(df.columns)}")

    return df

  except Exception as e:
    logging.error(f"Error extracting data from MongoDB: {str(e)}")
    raise

  finally:
    if client:
      client.close()
      logging.info("MongoDB connection closed")
