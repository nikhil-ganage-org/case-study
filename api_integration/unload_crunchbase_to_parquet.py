import requests
import pandas as pd
import time
import logging
import os
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException

# Configure logging
logging.basicConfig(filename='api_integration.log', level=logging.INFO)

# Constants
API_URL = 'https://api.crunchbase.com/v3.1/organizations/superside'
API_KEY = os.getenv('CRUNCHBASE_API_KEY')  # API key from environment variable
OUTPUT_FILE = 'superside_data.parquet'
RATE_LIMIT_WAIT = 1  # seconds

def fetch_data():
    headers = {
        'X-Cb-User-Key': API_KEY
    }
    params = {
        'user_key': API_KEY,
        'page': 1,
        'per_page': 100
    }
    
    all_data = []
    while True:
        try:
            response = requests.get(API_URL, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Extract relevant fields
            for organization in data.get('data', []):
                item = {
                    'permalink': organization.get('properties', {}).get('permalink', ''),
                    'website_url': organization.get('properties', {}).get('homepage_url', ''),
                    'updated_at': organization.get('properties', {}).get('updated_at', ''),
                    'linkedin_url': organization.get('properties', {}).get('linkedin_url', ''),
                    'city': organization.get('properties', {}).get('city_name', ''),
                    'region': organization.get('properties', {}).get('region_name', ''),
                    'country': organization.get('properties', {}).get('country_code', '')
                }
                all_data.append(item)
            
            # Check for pagination
            if 'next_page_url' in data.get('paging', {}):
                params['page'] += 1
            else:
                break
            
            # Respect rate limits
            time.sleep(RATE_LIMIT_WAIT)
        
        except HTTPError as http_err:
            logging.error(f"HTTP error occurred: {http_err}")
            break
        except ConnectionError as conn_err:
            logging.error(f"Connection error occurred: {conn_err}")
            time.sleep(5)
        except Timeout as timeout_err:
            logging.error(f"Timeout error occurred: {timeout_err}")
            time.sleep(5)
        except RequestException as req_err:
            logging.error(f"Request exception occurred: {req_err}")
            break
        except Exception as err:
            logging.error(f"An error occurred: {err}")
            break
    
    # Convert to DataFrame
    df = pd.DataFrame(all_data)
    
    # Write data to Parquet file
    write_to_parquet(df)
    logging.info(f"Data successfully written to {OUTPUT_FILE}.")

def write_to_parquet(df):
    try:
        df.to_parquet(OUTPUT_FILE, index=False)
        logging.info(f"Data written to Parquet file: {OUTPUT_FILE}")
    except Exception as e:
        logging.error(f"Error while writing to Parquet file: {e}")

if __name__ == "__main__":
    fetch_data()
    logging.info("API data fetch complete.")
