"""
Cranmore Snow Report Scraper
Purpose: Daily scraping of snow conditions from Cranmore (North Conway, New Hampshire)
    In accordance with cranmore.com terms of use

Stores raw data in Bronze layer for later transformation
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import json
import os
import boto3
from botocore.client import Config

default_args = {
    'owner': 'nettle',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def create_minio_client():
    """Create and return configured MinIO client"""
    return boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id=os.getenv('MINIO_ROOT_USER'),
        aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD'),
        # requests need to be cryptographically signed
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

def scrape_cranmore():
    """
    Scrape Cranmore snow report page
    Returns structured data with both metrics and narrative
    """
    url = 'https://cranmore.com/snow-report'
    
    # Set a proper User-Agent to identify the project
    headers = {
        'User-Agent': 'PowderPredictor/1.0 (Educational Project; [email protected])'
    }

    print(f"Fetching snow report from {url}")
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()  # Raise exception for bad status codes
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Initialize data structure
    data = {
        'mountain': 'cranmore',
        'scraped_at': datetime.now().isoformat(),
        'source_url': url,
    }

    # Extract structured metrics
    # Look for datapoint divs (how the 'conditions' appear on the page)
    try:
        datapoints = soup.find_all('div', class_='datapoint')
        
        for dp in datapoints:
            h3 = dp.find('h3')
            value = dp.find('span', class_='value')
            
            if h3 and value:
                label = h3.get_text(strip=True)
                val = value.get_text(strip=True)
                
                # Map labels to field names
                if 'Open Lifts' in label:
                    data['open_lifts'] = val
                elif 'Open Trails' in label:
                    data['open_trails'] = val
                elif 'Night Trails' in label:
                    data['night_trails'] = val
                elif '7 Day Total' in label:
                    data['seven_day_snowfall'] = val
                elif 'Season Total' in label:
                    data['season_total'] = val
        
        print(f"Extracted structured metrics: {len(data) - 3} fields")
    except Exception as e:
        print(f"Warning: Could not extract structured metrics: {e}")
        data['metrics_extraction_error'] = str(e)