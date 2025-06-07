from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import re
from datetime import datetime
import time
import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
import sys
import logging
from typing import Optional, List, Dict, Any
from ratelimit import limits, sleep_and_retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Rate limiting decorator - 1 call per second
@sleep_and_retry
@limits(calls=1, period=1)
def rate_limited_request(url: str, headers: Dict[str, str], params: Optional[Dict[str, Any]] = None) -> requests.Response:
    """Make a rate-limited request to the API."""
    return requests.get(url, headers=headers, params=params)

def get_version_data() -> Optional[List[Dict[str, str]]]:
    """
    Fetch version data from the CMS API documentation page.
    
    Returns:
        Optional[List[Dict[str, str]]]: List of dictionaries containing version and UUID pairs,
        or None if the fetch fails.
    """
    url = "https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/skilled-nursing-facility-all-owners/api-docs"
    
    logger.info(f"Attempting to fetch data from: {url}")
    
    # Set up headless Chrome
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--log-level=3')
    
    driver = None
    try:
        logger.info("Initializing Chrome driver...")
        driver = webdriver.Chrome(options=chrome_options)
        
        logger.info("Loading webpage...")
        driver.get(url)
        
        logger.info("Waiting for page to load...")
        try:
            wait = WebDriverWait(driver, 20)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            logger.info("Initial table element found.")
        except TimeoutException:
            logger.error("Timeout waiting for table element.")
            logger.debug(f"Page source preview: {driver.page_source[:1000]}")
            return None
        
        time.sleep(10)
        
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        tables = soup.find_all('table')
        
        if not tables:
            logger.error("No tables found in the page")
            return None
        
        version_table = None
        version_regex = re.compile(r"^[A-Za-z]+\s+\d{4}$")
        
        for i, table in enumerate(tables):
            headers = [h.text.strip() for h in table.find_all('th')]
            logger.debug(f"Checking table {i+1} with headers: {headers}")
            
            if 'Version' in headers and 'UUID' in headers:
                rows = table.find_all('tr')[1:]
                if rows:
                    first_col_text = rows[0].find_all('td')
                    if first_col_text and len(first_col_text) > 0:
                        version_text = first_col_text[0].text.strip()
                        if version_regex.match(version_text):
                            version_table = table
                            logger.info("Found version table with correct content format.")
                            break
        
        if not version_table:
            logger.error("Version table not found")
            return None
            
        version_data = []
        rows = version_table.find_all('tr')[1:]
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 2:
                version = cols[0].text.strip()
                uuid = cols[1].text.strip()
                if version and uuid:
                    version_data.append({
                        'version': version,
                        'uuid': uuid
                    })
                    logger.debug(f"Extracted: {version} - {uuid}")
        
        return version_data
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

def fetch_api_data(uuid: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """
    Fetch data from the CMS API using the provided UUID and parameters.
    
    Args:
        uuid: The API version UUID
        params: Dictionary of API parameters (optional)
    
    Returns:
        Optional[Dict[str, Any]]: JSON response data or None if the request fails
    """
    # Updated API endpoint format for CMS API
    base_url = f"https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/skilled-nursing-facility-all-owners/api/v1/{uuid}/data"
    
    # Default parameters if none provided
    if params is None:
        params = {}
    
    # Ensure we're requesting JSON format and add required parameters
    params.update({
        'format': 'json',
        'limit': 100,  # Number of records to return
        'offset': 0,   # Starting point for pagination
        'select': '*', # Select all columns
        'where': '1=1' # No filtering
    })
    
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        logger.info(f"Making request to: {base_url}")
        response = rate_limited_request(base_url, headers, params)
        
        # Log detailed response information
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response headers: {dict(response.headers)}")
        logger.info(f"Response content type: {response.headers.get('content-type', 'Not specified')}")
        
        # Check if response is empty
        if not response.text.strip():
            logger.error("Received empty response from API")
            return None
            
        # Log the first part of the response for debugging
        logger.info(f"Response preview: {response.text[:500]}")
            
        # Try to parse JSON response
        try:
            data = response.json()
            if not data:
                logger.error("Received empty JSON data from API")
                return None
            return data
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {str(e)}")
            logger.error(f"Response content: {response.text[:1000]}")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Error response content: {e.response.text[:1000]}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return None

def process_response_data(data: Optional[Dict[str, Any]]) -> Optional[pd.DataFrame]:
    """
    Process the API response data and convert it to a pandas DataFrame.
    
    Args:
        data: JSON response data from the API
    
    Returns:
        Optional[pd.DataFrame]: Processed DataFrame or None if processing fails
    """
    if not data:
        return None
    
    try:
        df = pd.DataFrame(data)
        df = df.replace('', pd.NA)
        return df
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        return None

def main():
    """Main function to execute the data fetching and processing workflow."""
    logger.info("Starting script...")
    
    # Get version data
    version_data = get_version_data()
    if not version_data:
        logger.error("Failed to retrieve version data")
        sys.exit(1)
    
    # Get the latest version
    latest_version = version_data[0]
    logger.info(f"Using latest version: {latest_version['version']}")
    
    # Add some basic parameters to the API request
    params = {
        'size': 100,  # Request 100 records at a time
        'offset': 0   # Start from the beginning
    }
    
    # Fetch data using the latest UUID
    api_data = fetch_api_data(latest_version['uuid'], params)
    if not api_data:
        logger.error("Failed to fetch API data")
        sys.exit(1)
    
    # Process the data
    df = process_response_data(api_data)
    if df is not None:
        logger.info(f"Successfully processed data. Shape: {df.shape}")
        # Save to CSV
        output_file = f"snf_ownership_data_{datetime.now().strftime('%Y%m%d')}.csv"
        df.to_csv(output_file, index=False)
        logger.info(f"Data saved to {output_file}")
    else:
        logger.error("Failed to process data")
        sys.exit(1)

if __name__ == "__main__":
    main()
    