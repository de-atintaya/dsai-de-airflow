import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
from typing import Dict, Any
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MySQLExtractor:
    def __init__(self):
        self.config = {
            'host': os.getenv('MYSQL_HOST'),
            'port': os.getenv('MYSQL_PORT'),
            'database': os.getenv('MYSQL_DATABASE'),
            'user': os.getenv('MYSQL_USER'),
            'password': os.getenv('MYSQL_PASSWORD'),
        }
        logger.info("MySQLExtractor initialized with configuration: %s", self.config)
    
    def get_connection(self):
        try:
            connection = mysql.connector.connect(**self.config)
            if connection.is_connected():
                logger.info("Successfully connected to MySQL database")
                return connection
        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            raise
    
    def execute_query(self, query: str) -> pd.DataFrame:
        connection = None
        try:
            connection = self.get_connection()
            df = pd.read_sql(query, connection)
            logger.info(f"Query executed successfully. Rows returned: {len(df)}")
            return df
        except Error as e:
            logger.error(f"Error executing query: {e}")
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()
                logger.info("MySQL connection closed")
    
    def extract_transactions(self) -> pd.DataFrame:
        query = """
        SELECT 
            t.transaction_id,
            t.account_id,
            t.date as transaction_date,
            t.amount,
            t.transaction_type,
            t.branch_id
        FROM transactions t
        ORDER BY t.date DESC, t.transaction_id
        """
        return self.execute_query(query)


def leer_datos_mysql() -> pd.DataFrame:
    extractor = MySQLExtractor()
    
    try:
        logger.info("Extracting transactions data from MySQL")
        return extractor.extract_transactions()
            
    except Exception as e:
        logger.error(f"Error in data extraction: {e}")
        raise