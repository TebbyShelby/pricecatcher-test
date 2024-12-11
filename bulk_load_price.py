import os
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import requests
import io
import duckdb
import pandas as pd
import json
import base64
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

def setup_drive_service(credentials_json):
    """Set up and return Google Drive service using service account json"""
    SCOPES = ['https://www.googleapis.com/auth/drive']
    
    with open('temp_credentials.json', 'w') as f:
        json.dump(credentials_json, f)
    
    credentials = service_account.Credentials.from_service_account_file(
        'temp_credentials.json', scopes=SCOPES)
    
    os.remove('temp_credentials.json')
    return build('drive', 'v3', credentials=credentials)

def download_from_drive(service, file_id, local_path):
    """Download file from Google Drive"""
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    
    fh.seek(0)
    with open(local_path, 'wb') as f:
        f.write(fh.read())

def upload_to_drive(service, file_path, file_id):
    """Update existing file in Google Drive"""
    media = MediaFileUpload(file_path, resumable=True)
    service.files().update(
        fileId=file_id,
        media_body=media
    ).execute()

def download_parquet(url, local_filename):
    """Download parquet file from URL"""
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        os.makedirs('temp', exist_ok=True)
        local_path = os.path.join('temp', local_filename)
        
        with open(local_path, 'wb') as f:
            f.write(response.content)
        
        return local_path
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"File not found at URL: {url}")
            return None
        raise

def cleanup_temp_files():
    """Clean up temporary directory"""
    if os.path.exists('temp'):
        for file in os.listdir('temp'):
            os.remove(os.path.join('temp', file))
        os.rmdir('temp')

def process_monthly_file(conn, year_month, url):
    """Process an entire monthly parquet file"""
    print(f"Processing data for {year_month} from {url}")
    
    # Download the monthly file
    local_path = download_parquet(url, f"pricecatcher_{year_month}.parquet")
    if not local_path:
        return None
    
    # Load data and get unique dates
    conn.execute("CREATE TEMPORARY TABLE temp_price AS SELECT * FROM read_parquet(?)", [local_path])
    
    # Insert data with deduplication
    result = conn.execute("""
        WITH new_records AS (
            SELECT DISTINCT t.* 
            FROM temp_price t
            WHERE NOT EXISTS (
                SELECT 1 
                FROM price x 
                WHERE x.date = t.date
                AND x.premise_code = t.premise_code 
                AND x.item_code = t.item_code
            )
        )
        INSERT INTO price 
        SELECT * FROM new_records
    """)
    
    # Get unique dates from the loaded data
    dates = conn.execute("""
        SELECT DISTINCT date as processed_date
        FROM temp_price
        ORDER BY processed_date
    """).fetchall()
    
    # Drop temporary table
    conn.execute("DROP TABLE temp_price")
    
    return {
        'records_loaded': result.rowcount,
        'dates': [d[0] for d in dates]
    }

def generate_month_urls(start_date, end_date):
    """Generate list of monthly URLs between start and end dates"""
    current_date = start_date.replace(day=1)
    urls = []
    
    while current_date <= end_date:
        urls.append({
            'url': f"https://storage.data.gov.my/pricecatcher/pricecatcher_{current_date.strftime('%Y-%m')}.parquet",
            'year_month': current_date.strftime('%Y-%m')
        })
        current_date += relativedelta(months=1)
    
    return urls

def main():
    # Configuration
    DUCKDB_FILE_ID = "1L0E2fSEAYrpzHV3Jwt1nznjTUJAKQcV_"
    START_DATE = date(2022, 1, 1)  # Start from January 2022
    END_DATE = date.today()  # Until today
    
    # Get service account credentials from environment
    credentials_json = json.loads(
        base64.b64decode(os.environ['GOOGLE_CREDENTIALS']).decode('utf-8')
    )
    
    try:
        # Setup Drive service
        print("Setting up Drive service...")
        service = setup_drive_service(credentials_json)
        
        # Create temp directory
        os.makedirs('temp', exist_ok=True)
        
        # Download existing DuckDB file
        print("Downloading existing DuckDB file...")
        duckdb_path = os.path.join('temp', 'pricecatcher.duckdb')
        download_from_drive(service, DUCKDB_FILE_ID, duckdb_path)
        
        # Connect to DuckDB
        conn = duckdb.connect(duckdb_path)
        
        # Create tables if they don't exist
        conn.execute("""
            CREATE TABLE IF NOT EXISTS price (
                date DATE,
                premise_code VARCHAR,
                item_code VARCHAR,
                price DECIMAL(10,2),
                PRIMARY KEY (date, premise_code, item_code)
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS data_processing_log (
                processed_date DATE PRIMARY KEY,
                processed_at TIMESTAMP,
                file_url VARCHAR,
                records_loaded INTEGER
            )
        """)
        
        # Generate list of monthly URLs
        monthly_urls = generate_month_urls(START_DATE, END_DATE)
        
        total_records = 0
        for month_data in monthly_urls:
            result = process_monthly_file(conn, month_data['year_month'], month_data['url'])
            
            if result and result['dates']:
                total_records += result['records_loaded']
                # Update processing log for each date in the month
                for processed_date in result['dates']:
                    conn.execute("""
                        INSERT INTO data_processing_log (processed_date, processed_at, file_url, records_loaded)
                        VALUES (?, CURRENT_TIMESTAMP, ?, ?)
                        ON CONFLICT (processed_date) DO UPDATE SET
                        processed_at = CURRENT_TIMESTAMP,
                        records_loaded = excluded.records_loaded
                    """, [processed_date, month_data['url'], result['records_loaded']])
        
        print(f"Total records loaded: {total_records}")
        
        conn.close()
        
        # Upload updated DuckDB back to Drive
        print("Uploading updated DuckDB file...")
        upload_to_drive(service, duckdb_path, DUCKDB_FILE_ID)
        
        # Cleanup
        print("Cleaning up...")
        cleanup_temp_files()
        
        print("Bulk load completed successfully!")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        cleanup_temp_files()
        raise e

if __name__ == "__main__":
    main()