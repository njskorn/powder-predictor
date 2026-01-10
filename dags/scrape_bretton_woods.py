"""
Bretton Woods Snow Report Scraper
Purpose: Daily scraping of snow conditions from Bretton Woods Mountain Resort
Stores raw data in Bronze layer for later transformation
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import json
import boto3
from botocore.client import Config
import os

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
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

def scrape_bretton_woods():
    """
    Scrape Bretton Woods snow report page
    Returns structured data with both metrics and narrative
    """
    url = 'https://www.brettonwoods.com/snow-trail-report/'
    
    headers = {
        'User-Agent': 'PowderPredictor/1.0 (Educational Project; [email protected])'
    }
    
    print(f"Fetching snow report from {url}")
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    data = {
        'mountain': 'bretton-woods',
        'scraped_at': datetime.now().isoformat(),
        'source_url': url,
    }
    
    # Extract top of the page Snow Report metrics
    try:
        # Extract summary items (Trail Count, Glades Count, Lifts Open, etc.)
        summary_items = soup.find_all('div', class_='trail-reports__summary-item')
        
        for item in summary_items:
            heading = item.find('div', class_='trail-reports__summary-item-heading')
            count = item.find('div', class_='trail-reports__summary-item-count')
            
            if heading and count:
                heading_text = heading.get_text(strip=True)
                count_text = count.get_text(strip=True)
                
                # Map heading to field name
                if 'Trail Count' in heading_text:
                    data['open_trails'] = count_text
                elif 'Glades Count' in heading_text:
                    data['glades_open'] = count_text
                elif 'Lifts Open' in heading_text:
                    data['open_lifts'] = count_text
                elif 'Total Acreage' in heading_text:
                    data['total_acreage'] = count_text
                elif 'Total Miles' in heading_text:
                    data['total_miles'] = count_text
        
        # Extract 'info' items (Trail Acreage, Glade Acreage, Lift Hours, Grooming, etc.)
        info_items = soup.find_all('div', class_='trail-reports__info-item')
        
        for item in info_items:
            heading = item.find('div', class_='trail-reports__info-item-heading')
            value = item.find('div', class_='trail-reports__info-item-value')
            
            if heading and value:
                heading_text = heading.get_text(strip=True).replace(':', '').strip()
                value_text = value.get_text(strip=True)
                
                # Map heading to field name
                if 'Trail Acreage' in heading_text:
                    data['trail_acreage'] = value_text
                elif 'Glade Acreage' in heading_text:
                    data['glade_acreage'] = value_text
                elif 'Lift Hours' in heading_text:
                    data['lift_hours'] = value_text
                elif 'Grooming' in heading_text:
                    data['grooming'] = value_text
                elif 'Snowmaking' in heading_text:
                    data['snowmaking'] = value_text
                elif 'Snow Conditions' in heading_text:
                    data['snow_conditions'] = value_text
                elif 'Base Depth' in heading_text:
                    data['base_depth'] = value_text
        
        # Extract snowfall data from the table
        snowfall_rows = soup.find_all('div', class_='trail-reports__snowfall-row')
        
        for row in snowfall_rows:
            cells = row.find_all('div', class_='trail-reports__snowfall-cell')
            
            if len(cells) >= 3:  # Label + 2 values (base and upper)
                label = cells[0].get_text(strip=True)
                value1 = cells[1].get_text(strip=True) if len(cells) > 1 else ''
                value2 = cells[2].get_text(strip=True) if len(cells) > 2 else ''
                
                if 'Recent' in label:
                    data['recent_snowfall_base'] = value1
                    data['recent_snowfall_upper'] = value2
                elif 'Season to Date' in label or 'Season' in label:
                    data['season_snowfall_base'] = value1
                    data['season_snowfall_upper'] = value2
        
        print(f"Extracted structured metrics: {len([k for k in data.keys() if k not in ['mountain', 'scraped_at', 'source_url']])} fields")
    except Exception as e:
        print(f"Warning: Could not extract all metrics: {e}")
        data['metrics_extraction_error'] = str(e)
    
    # Extract last updated timestamp
    try:
        updated_elem = soup.find(string=lambda text: text and 'Updated:' in text)
        if updated_elem:
            data['last_updated'] = updated_elem.strip()
    except Exception as e:
        print(f"Warning: Could not extract last updated: {e}")
    
    # Extract narrative/comments
    try:
        # Look for "Comments" section
        comments_header = soup.find(string=lambda text: text and 'Comments' in text)
        if comments_header:
            # Get the parent and following siblings/content
            parent = comments_header.find_parent()
            if parent:
                # Get all text from the comments section
                report_parts = []
                for elem in parent.find_all(['p', 'div'], recursive=True):
                    text = elem.get_text(strip=True)
                    if text and len(text) > 20:  # Filter out short/empty elements
                        report_parts.append(text)
                
                if report_parts:
                    data['report_text'] = ' '.join(report_parts[:5])  # Take first 5 substantial paragraphs
                    print(f"Extracted narrative report ({len(data.get('report_text', ''))} chars)")
    except Exception as e:
        print(f"Warning: Could not extract narrative text: {e}")
        data['narrative_extraction_error'] = str(e)
    
    print(f"Scraping complete! Collected {len(data)} fields")
    return data

def save_to_bronze(**context):
    """
    Save scraped data to Bronze layer in MinIO
    """
    # let Airflow pass data between task instances
    ti = context['ti']
    data = ti.xcom_pull(task_ids='scrape_snow_report')
    
    if not data:
        raise ValueError("No data received from scraping task")
    
    s3 = create_minio_client()
    
    date_str = datetime.now().strftime('%Y-%m-%d')
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    bucket_name = 'bronze-snow-reports'
    object_key = f'bretton-woods/daily/{date_str}/{timestamp_str}.json'
    
    json_data = json.dumps(data, indent=2)
    
    s3.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=json_data.encode('utf-8'),
        ContentType='application/json',
        Metadata={
            'mountain': 'bretton-woods',
            'scrape_date': date_str,
        }
    )
    
    print(f"Saved to Bronze layer: s3://{bucket_name}/{object_key}")
    print(f"  File size: {len(json_data)} bytes")
    
    return object_key

# Define the DAG
with DAG(
    dag_id='scrape_bretton_woods',
    default_args=default_args,
    description='Daily scrape of Bretton Woods snow report',
    schedule='0 12 * * *',  # Run daily at 12 PM UTC (7 AM EST)
    start_date=datetime(2025, 1, 9),
    catchup=False,
    tags=['scraper', 'bretton-woods', 'bronze'],
) as dag:
    
    scrape_task = PythonOperator(
        task_id='scrape_snow_report',
        python_callable=scrape_bretton_woods,
    )
    
    save_task = PythonOperator(
        task_id='save_to_bronze',
        python_callable=save_to_bronze,
    )
    
    scrape_task >> save_task