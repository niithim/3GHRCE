import mysql.connector
from mysql.connector import Error
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def setup_database():
    try:
        # First connect without database
        connection = mysql.connector.connect(
            host='localhost',
            port=3306,
            user='root',
            password='Nithin@123'
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Create database
            cursor.execute("CREATE DATABASE IF NOT EXISTS cms_data")
            logger.info("Database 'cms_data' created successfully")
            
            # Use the database
            cursor.execute("USE cms_data")
            
            # Create tables for each dataset
            tables = [
                "skilled_nursing_facility_all_owners",
                "skilled_nursing_facility_enrollments",
                "skilled_nursing_facility_change_of_ownership",
                "provider_information"
            ]
            
            for table in tables:
                cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
                logger.info(f"Dropped existing table {table} if it existed")
            
            connection.commit()
            logger.info("Database setup completed successfully")
            
    except Error as e:
        logger.error(f"Error while setting up database: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            logger.info("MySQL connection closed")

if __name__ == "__main__":
    setup_database() 