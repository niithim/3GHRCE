import mysql.connector
from mysql.connector import Error
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def test_mysql_connection():
    try:
        # Connect to MySQL
        connection = mysql.connector.connect(
            host='localhost',
            port=3306,
            user='root',
            password='Nithin@123',
            database='cms_data'
        )
        
        if connection.is_connected():
            db_info = connection.server_info
            logger.info(f"Connected to MySQL Server version {db_info}")
            
            cursor = connection.cursor()
            
            # Show all databases
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()
            logger.info("\nAvailable databases:")
            for db in databases:
                logger.info(f"- {db[0]}")
            
            # Show tables in cms_data
            cursor.execute("USE cms_data")
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            logger.info("\nTables in cms_data database:")
            for table in tables:
                table_name = table[0]
                logger.info(f"- {table_name}")
                
                # Show table structure
                cursor.execute(f"DESCRIBE {table_name}")
                columns = cursor.fetchall()
                logger.info(f"\nStructure of {table_name}:")
                for column in columns:
                    logger.info(f"- {column[0]}: {column[1]}")
            
    except Error as e:
        logger.error(f"Error while connecting to MySQL: {e}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            logger.info("\nMySQL connection closed")

if __name__ == "__main__":
    test_mysql_connection() 