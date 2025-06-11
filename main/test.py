import asyncio
from playwright.async_api import async_playwright
from io import StringIO
import re
import logging
import httpx
import pandas as pd
from typing import Optional
from urllib.parse import urlparse
import mysql.connector
from mysql.connector import Error

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# MySQL connection
def get_mysql_connection():
    try:
        # First connect without database to create it if needed
        initial_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'Nithin@123',
            'raise_on_warnings': False
        }
        logger.info("Attempting to connect to MySQL server...")
        initial_connection = mysql.connector.connect(**initial_config)
        
        # Create database if it doesn't exist
        cursor = initial_connection.cursor()
        try:
            cursor.execute("CREATE DATABASE IF NOT EXISTS cms_data")
            logger.info("Database cms_data is ready.")
        except Error as e:
            if e.errno == 1007:  # Database exists error
                logger.info("Database cms_data already exists.")
            else:
                raise
        cursor.close()
        initial_connection.close()
        
        # Now connect with the database
        config = {
            'host': 'localhost',
            'port': 3306,
            'database': 'cms_data',
            'user': 'root',
            'password': 'Nithin@123',
            'raise_on_warnings': False
        }
        logger.info("Connecting to cms_data database...")
        connection = mysql.connector.connect(**config)
        if connection.is_connected():
            logger.info("Connected to MySQL database cms_data.")
            return connection
    except Error as e:
        logger.error(f"Error connecting to MySQL: {e}")
        return None

# Always drop and recreate the table for type safety, all columns as TEXT
def create_table_if_not_exists(connection, table_name, df):
    cursor = connection.cursor()
    try:
        database_name = connection.database
        logger.info(f"Creating table {table_name} in database {database_name}")
        
        # Drop table if exists
        try:
            cursor.execute(f"DROP TABLE IF EXISTS `{database_name}`.`{table_name}`")
            logger.info(f"Dropped existing table {table_name} if it existed")
        except Error as e:
            logger.warning(f"Warning while dropping table {table_name}: {e}")

        # Rename long columns
        col_name_map = {}
        used_names = set()
        for col in df.columns:
            if len(col) > 64:
                base = col[:60]
                suffix = 1
                new_col = f"{base}_{suffix}"
                while new_col in used_names:
                    suffix += 1
                    new_col = f"{base}_{suffix}"
                col_name_map[col] = new_col
                used_names.add(new_col)
            else:
                col_name_map[col] = col
                used_names.add(col)
        
        if col_name_map:
            logger.info(f"Renamed {len(col_name_map)} columns to fit MySQL constraints")
            df.rename(columns=col_name_map, inplace=True)

        # Force all to string
        for col in df.columns:
            df[col] = df[col].astype(str)

        columns = [f"`{col}` TEXT" for col in df.columns]
        create_table_query = f"""
        CREATE TABLE `{database_name}`.`{table_name}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {', '.join(columns)},
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table_query)
        connection.commit()
        logger.info(f"Table `{table_name}` created successfully with {len(columns)} columns")
    except Error as e:
        logger.error(f"Error creating table {table_name}: {e}")
        raise
    finally:
        cursor.close()

# Insert dataframe data into MySQL table
def insert_data_to_mysql(connection, table_name, df):
    cursor = connection.cursor()
    try:
        database_name = connection.database
        columns = [f"`{col}`" for col in df.columns]
        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = f"INSERT INTO `{database_name}`.`{table_name}` ({', '.join(columns)}) VALUES ({placeholders})"

        # Replace NaN with empty strings to avoid NULL issues
        values = [tuple(row) for row in df.fillna("").values]

        cursor.executemany(insert_query, values)
        connection.commit()
        logger.info(f"{len(values)} rows inserted into `{table_name}`.")
    except Exception as e:
        logger.error(f"Error inserting data into `{table_name}`: {e}")
        raise
    finally:
        cursor.close()

# Extract dataset slug from API docs URL
def extract_slug_from_url(url: str) -> str:
    path = urlparse(url).path
    slug = path.strip("/").split("/")[-2]
    return slug.replace("-", "_")

# Use Playwright to get dataset UUID from api-docs URL
async def get_dataset_uuid(api_docs_url: str) -> Optional[str]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        requests = []
        page.on("request", lambda request: requests.append(request.url))
        logger.info(f"Opening: {api_docs_url}")
        await page.goto(api_docs_url, wait_until="networkidle")
        await page.wait_for_timeout(5000)
        await browser.close()
        for url in requests:
            match = re.search(r'/dataset/([a-z0-9-]{36})', url)
            if match:
                dataset_uuid = match.group(1)
                logger.info(f"Dataset UUID found: {dataset_uuid}")
                return dataset_uuid
        logger.error("Dataset UUID not found.")
        return None

# Fetch dataset pages via API and combine data
async def fetch_data(dataset_uuid: str, page_size: int = 5000, max_pages: int = 10) -> Optional[list]:
    all_data = []
    base_url = f"https://data.cms.gov/data-api/v1/dataset/{dataset_uuid}/data"
    headers = {'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0'}
    async with httpx.AsyncClient(timeout=30.0) as client:
        for page_num in range(max_pages):
            url = f"{base_url}?page[number]={page_num}&page[size]={page_size}"
            try:
                response = await client.get(url, headers=headers)
                if response.status_code == 200:
                    records = response.json()
                    if isinstance(records, dict) and "data" in records:
                        records = records["data"]
                    logger.info(f"Fetched {len(records)} records from page {page_num}")
                    if not records:
                        break
                    all_data.extend(records)
                else:
                    logger.error(f"Failed to fetch page {page_num}: {response.status_code}")
                    break
            except Exception as e:
                logger.error(f"Exception while fetching page {page_num}: {str(e)}")
                break
    return all_data if all_data else None

# Process dataset given an api-docs URL
async def process_dataset(api_docs_url: str):
    dataset_slug = extract_slug_from_url(api_docs_url)
    # Special case for "change-of-ownership" dataset
    if "change-of-ownership" in api_docs_url:
        dataset_slug = "change_of_ownership"

    dataset_uuid = await get_dataset_uuid(api_docs_url)
    if not dataset_uuid:
        return

    data = await fetch_data(dataset_uuid)
    if not data:
        logger.error(f"No data found for {dataset_slug}.")
        return

    df = pd.DataFrame(data)

    # Force all columns to string (TEXT)
    for col in df.columns:
        df[col] = df[col].astype(str)

    print(f"\nPreview of {dataset_slug} data:")
    print(df.head())

    csv_filename = f"{dataset_slug}.csv"
    df.to_csv(csv_filename, index=False)
    logger.info(f"Data saved to CSV: {csv_filename}")

    conn = get_mysql_connection()
    if conn:
        try:
            create_table_if_not_exists(conn, dataset_slug, df)
            insert_data_to_mysql(conn, dataset_slug, df)
        finally:
            conn.close()
            logger.info("MySQL connection closed.")

# Process direct CSV dataset from dataset ID (like "4pq5-n9py")
async def process_direct_csv_dataset(dataset_id: str, dataset_slug: Optional[str] = None):
    if not dataset_slug:
        dataset_slug = "provider"

    metadata_url = f"https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/{dataset_id}?show-reference-ids=false"

    async with httpx.AsyncClient(timeout=30.0) as client:
        logger.info(f"Fetching metadata for dataset {dataset_id}")
        try:
            response = await client.get(metadata_url, headers={"Accept": "application/json"})
            response.raise_for_status()
            metadata = response.json()
        except Exception as e:
            logger.error(f"Failed to retrieve metadata: {e}")
            return

        try:
            distributions = metadata.get("distribution", [])
            if not distributions:
                logger.error("No distributions found in metadata")
                return

            download_url = distributions[0]["data"]["downloadURL"]
            logger.info(f"CSV download URL: {download_url}")
        except (KeyError, IndexError) as e:
            logger.error(f"Error extracting download URL: {e}")
            return

        try:
            logger.info("Downloading CSV file...")
            csv_response = await client.get(download_url)
            csv_response.raise_for_status()
            csv_data = csv_response.text
            df = pd.read_csv(StringIO(csv_data))
            logger.info(f"CSV loaded with shape: {df.shape}")
        except Exception as e:
            logger.error(f"Failed to download or parse CSV: {e}")
            return

    # Force all columns to string (TEXT)
    for col in df.columns:
        df[col] = df[col].astype(str)

    output_file = f"{dataset_slug}.csv"
    df.to_csv(output_file, index=False)
    logger.info(f"CSV data saved to {output_file}")

    conn = get_mysql_connection()
    if conn:
        try:
            create_table_if_not_exists(conn, dataset_slug, df)
            insert_data_to_mysql(conn, dataset_slug, df)
        finally:
            conn.close()
            logger.info("MySQL connection closed.")

# Process state average data
async def process_state_average_dataset():
    dataset_slug = "state_average"
    # Using the correct API endpoint for dataset metadata
    metadata_url = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/xcdc-v8bm?show-reference-ids=false"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            logger.info("Fetching state average data...")
            response = await client.get(metadata_url)
            response.raise_for_status()
            metadata = response.json()
            
            # Log the metadata structure to understand what we're getting
            logger.info(f"Metadata structure: {metadata.keys()}")
            
            # Get the download URL from metadata
            download_url = None
            if 'distribution' in metadata:
                distributions = metadata['distribution']
                if distributions and len(distributions) > 0:
                    download_url = distributions[0].get('data', {}).get('downloadURL')
            
            if not download_url:
                logger.error("Could not find download URL in metadata")
                logger.error(f"Available metadata keys: {metadata.keys()}")
                return
                
            logger.info(f"State average CSV download URL: {download_url}")
            
            # Download and process the CSV
            csv_response = await client.get(download_url)
            csv_response.raise_for_status()
            df = pd.read_csv(StringIO(csv_response.text))
            
            # Convert all columns to string
            for col in df.columns:
                df[col] = df[col].astype(str)
            
            # Save to CSV
            df.to_csv(f"{dataset_slug}.csv", index=False)
            logger.info(f"State average data saved to {dataset_slug}.csv")
            
            # Save to MySQL
            conn = get_mysql_connection()
            if conn:
                try:
                    create_table_if_not_exists(conn, dataset_slug, df)
                    insert_data_to_mysql(conn, dataset_slug, df)
                finally:
                    conn.close()
                    logger.info("MySQL connection closed for state average data.")
        except Exception as e:
            logger.error(f"Error processing state average data: {e}")
            logger.error(f"Full error details: {str(e)}")

# Main execution function
async def main():
    api_docs_urls = [
        # 1. SNF All Owners
        "https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/skilled-nursing-facility-all-owners/api-docs",

        # 2. SNF Enrollments
        "https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/skilled-nursing-facility-enrollments/api-docs",

        # 3. SNF Change of Ownership
        "https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/skilled-nursing-facility-change-of-ownership/api-docs",

        # 4. SNF Entity Performance
        "https://data.cms.gov/quality-of-care/nursing-home-affiliated-entity-performance-measures/api-docs",

        # 5. SNF Cost Report (API-based method)
        "https://data.cms.gov/provider-compliance/cost-report/skilled-nursing-facility-cost-report/api-docs"
    ]

    # Process all API-based datasets
    for url in api_docs_urls:
        await process_dataset(url)

    # 6. SNF Provider Info — Direct CSV dataset
    await process_direct_csv_dataset("4pq5-n9py", dataset_slug="provider")
    
    
    #7. Process state average data
    await process_state_average_dataset()
    


if __name__ == "__main__":
    asyncio.run(main())
