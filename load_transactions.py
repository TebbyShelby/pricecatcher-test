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
    response = requests.get(url)
    response.raise_for_status()
    
    os.makedirs('temp', exist_ok=True)
    local_path = os.path.join('temp', local_filename)
    
    with open(local_path, 'wb') as f:
        f.write(response.content)
    
    return local_path

def cleanup_temp_files():
    """Clean up temporary directory"""
    if os.path.exists('temp'):
        for file in os.listdir('temp'):
            os.remove(os.path.join('temp', file))
        os.rmdir('temp')

def main():
    # Configuration
    DUCKDB_FILE_ID = "1L0E2fSEAYrpzHV3Jwt1nznjTUJAKQcV_"
    TRANSACTION_URL = "https://storage.data.gov.my/pricecatcher/pricecatcher_2022-02.parquet"  # January 2022
    
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
        
        # Download transaction parquet
        print("Downloading transaction data...")
        transaction_path = download_parquet(TRANSACTION_URL, "pricecatcher_2022-02.parquet")
        
        # Connect to DuckDB and load transaction data
        print("Loading transaction data into DuckDB...")
        conn = duckdb.connect(duckdb_path)
        
        # Create transaction table if it doesn't exist
        conn.execute("""
            CREATE TABLE IF NOT EXISTS price (
                date DATE,
                premise_code VARCHAR,
                item_code VARCHAR,
                price DECIMAL(10,2)
            )
        """)
        
        # Load new data
        conn.execute("""
            INSERT INTO price 
            SELECT * FROM read_parquet(?)
        """, [transaction_path])
        
        conn.close()
        
        # Upload updated DuckDB back to Drive
        print("Uploading updated DuckDB file...")
        upload_to_drive(service, duckdb_path, DUCKDB_FILE_ID)
        
        # Cleanup
        print("Cleaning up...")
        cleanup_temp_files()
        
        print("Transaction data loaded successfully!")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        cleanup_temp_files()
        raise e

if __name__ == "__main__":
    main()