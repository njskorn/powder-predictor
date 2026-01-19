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

    # Extract last updated date and report text
    try:
        # Find the "Last Updated:" text
        last_updated_elem = soup.find(string=lambda text: text and 'Last Updated:' in text)
        if last_updated_elem:
            data['last_updated'] = last_updated_elem.strip()
        
        # Find the report text
        # Look for the "Report:" label and get the following text
        report_elem = soup.find(string=lambda text: text and 'Report:' in text)
        if report_elem:
            # Get the parent and find all following paragraphs/text
            parent = report_elem.parent
            report_text = []
            
            # Get all text after "Report:"
            for sibling in report_elem.find_next_siblings():
                # Stop when we hit the structured data tables
                if sibling.name == 'div':
                    break
                
                # Only process paragraph tags
                if sibling.name == 'p':
                    text = sibling.get_text(strip=True)
                    if text and text != 'Report:':
                        report_text.append(text)
            
            data['report_text'] = ' '.join(report_text)
        
        print(f"Extracted narrative report ({len(data.get('report_text', ''))} chars)")
    except Exception as e:
        print(f"Warning: Could not extract narrative text: {e}")
        data['narrative_extraction_error'] = str(e)
    
    print(f"✓ Scraping complete! Collected {len(data)} fields")
    return data

def save_to_bronze(**context):
    """
    Save daily scraped data from Cranmore to Bronze layer in MinIO
    """
    # Get data from previous task via XCom (Airflow's method of passing data between tasks)
    ti = context['ti'] # task instance
    data = ti.xcom_pull(task_ids='scrape_snow_report') # get data from prior task, see DAG
    
    if not data:
        raise ValueError("No data received from scraping task")
    
    s3 = create_minio_client()
    
    # Create filename with date
    date_str = datetime.now().strftime('%Y-%m-%d')
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    bucket_name = 'bronze-snow-reports'
    object_key = f'cranmore/daily/{date_str}/{timestamp_str}.json'
    
    # Convert to JSON
    json_data = json.dumps(data, indent=2)
    
    # Upload to MinIO
    s3.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=json_data.encode('utf-8'),
        ContentType='application/json',
        Metadata={
            'mountain': 'cranmore',
            'scrape_date': date_str,
        }
    )
    
    print(f"Saved to Bronze layer: s3://{bucket_name}/{object_key}")
    print(f"File size: {len(json_data)} bytes")
    
    return object_key

# Define the DAG
with DAG(
    dag_id='scrape_cranmore',
    default_args=default_args,
    description='Daily scrape of Cranmore snow report',
    schedule='0 12 * * *',  # Run daily at 12 PM UTC (7 AM EST)
    start_date=datetime(2025, 1, 10),
    catchup=False,
    tags=['scraper', 'cranmore', 'bronze'],
) as dag:
    
    # Task 1: Scrape the website
    scrape_task = PythonOperator(
        task_id='scrape_snow_report',
        python_callable=scrape_cranmore,
    )
    
    # Task 2: Save to Bronze layer
    save_task = PythonOperator(
        task_id='save_to_bronze',
        python_callable=save_to_bronze,
    )
    
    # Define dependency
    scrape_task >> save_task