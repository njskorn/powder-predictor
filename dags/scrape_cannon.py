"""
Cannon Mountain Snow Report Scraper
Purpose: Daily scraping of snow conditions from Cannon Mountain
Scrapes both mountain-report and trails-lifts pages
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
import re

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

def scrape_mountain_report():
    """
    Scrape the mountain-report page for conditions and snowfall
    Returns basic metrics and narrative
    """
    url = 'https://www.cannonmt.com/mountain-report'
    
    headers = {
        'User-Agent': 'PowderPredictor/1.0 (Educational Project; [email protected])'
    }
    
    print(f"Fetching mountain report from {url}")
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    data = {}
    
    # Extract season snowfall (big number at top)
    try:
        # Find the div with class "label" containing "SNOWFALL TO DATE"
        label_div = soup.find('div', class_='label', string='SNOWFALL TO DATE')
        if label_div:
            # The number is in a sibling div with font-swiss-outline class
            parent = label_div.find_parent()
            if parent:
                number_div = parent.find('div', class_=lambda x: x and 'font-swiss-outline' in x)
                if number_div:
                    data['season_snowfall'] = number_div.get_text(strip=True)
                    print(f"Extracted season snowfall: {data['season_snowfall']}")
    except Exception as e:
        print(f"Warning: Could not extract season snowfall: {e}")
    
    # Extract surface conditions
    try:
        # Look for "PRIMARY SURFACE" and "SECONDARY SURFACE"
        primary = soup.find(string=lambda text: text and 'PRIMARY SURFACE' in text.upper())
        if primary:
            parent = primary.find_parent()
            if parent:
                # Get the next text element
                conditions_elem = parent.find_next(string=True)
                if conditions_elem:
                    data['primary_surface'] = conditions_elem.strip()
        
        secondary = soup.find(string=lambda text: text and 'SECONDARY SURFACE' in text.upper())
        if secondary:
            parent = secondary.find_parent()
            if parent:
                conditions_elem = parent.find_next(string=True)
                if conditions_elem:
                    data['secondary_surface'] = conditions_elem.strip()
    except Exception as e:
        print(f"Warning: Could not extract surface conditions: {e}")
    
    # Extract last updated timestamp
    try:
        updated = soup.find(string=lambda text: text and 'Last Updated:' in text)
        if updated:
            data['last_updated'] = updated.strip()
    except Exception as e:
        print(f"Warning: Could not extract last updated: {e}")
    
    # Extract narrative report (the main text block)
    try:
        # The narrative is in a section with class "parse-text" inside the main content
        parse_text_section = soup.find('div', class_='parse-text')
        if parse_text_section:
            # Get all paragraph text
            paragraphs = parse_text_section.find_all('p')
            narrative_parts = []
            
            for p in paragraphs:
                text = p.get_text(strip=True)
                # Take substantial paragraphs, skip very short ones
                if len(text) > 30:
                    narrative_parts.append(text)
            
            if narrative_parts:
                # Take first 5-6 paragraphs for a good summary
                data['report_text'] = ' '.join(narrative_parts[:6])
                print(f"Extracted narrative report ({len(data.get('report_text', ''))} chars)")
        else:
            print("Warning: Could not find parse-text section")
    except Exception as e:
        print(f"Warning: Could not extract narrative text: {e}")
    
    print(f"Mountain report scraped: {len(data)} fields")
    return data

def scrape_trails_lifts():
    """
    Scrape the trails-lifts page for detailed trail/lift status
    Returns counts and individual trail details
    """
    url = 'https://www.cannonmt.com/trails-lifts'
    
    headers = {
        'User-Agent': 'PowderPredictor/1.0 (Educational Project; [email protected])'
    }
    
    print(f"Fetching trails/lifts from {url}")
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    data = {
        'trails': [],
        'lifts': []
    }
    
    # Extract summary numbers from top of page
    try:
        # Find all label divs and their associated numbers
        labels = soup.find_all('div', class_='label')
        
        for label in labels:
            label_text = label.get_text(strip=True).upper()
            parent = label.find_parent()
            
            if parent:
                number_div = parent.find('div', class_=lambda x: x and 'font-swiss-outline' in x)
                if number_div:
                    value = number_div.get_text(strip=True)
                    
                    if label_text == 'TRAILS':
                        data['trails_open'] = value
                        print(f"Extracted trails: {value}")
                    elif label_text == 'LIFTS':
                        data['lifts_open'] = value
                        print(f"Extracted lifts: {value}")
                    elif label_text == 'ACRES':
                        data['acres'] = value
                        print(f"Extracted acres: {value}")
                    elif label_text == 'MILES':
                        data['miles'] = value
                        print(f"Extracted miles: {value}")
    except Exception as e:
        print(f"Warning: Could not extract summary numbers: {e}")
    
    # Extract individual trail status
    try:
        # Find all trail rows
        trail_rows = soup.find_all('tr', class_=lambda x: x and 'flex items-center' in x)
        
        current_area = None
        
        for row in trail_rows:
            # Check if this is an area header (h3)
            area_header = row.find_previous('h3')
            if area_header and area_header != current_area:
                current_area = area_header.get_text(strip=True)
            
            # Extract trail data from the row
            trail_info = {}
            
            # Status (open/closed) from first SVG aria-label
            status_svg = row.find('svg', {'aria-label': True})
            if status_svg:
                aria_label = status_svg.get('aria-label', '')
                if 'checkmark' in aria_label.lower():
                    trail_info['status'] = 'open'
                elif 'x' in aria_label.lower() or 'closed' in aria_label.lower():
                    trail_info['status'] = 'closed'
            
            # Grooming status from groomer icon
            groomer_svg = row.find('svg', {'aria-label': lambda x: x and 'groomer' in x.lower()})
            trail_info['groomed'] = groomer_svg is not None
            
            # Difficulty from difficulty icon
            difficulty_svg = row.find('svg', {'aria-label': lambda x: x and any(d in x.lower() for d in ['circle', 'square', 'diamond'])})
            if difficulty_svg:
                difficulty_label = difficulty_svg.get('aria-label', '').lower()
                if 'circle' in difficulty_label:
                    trail_info['difficulty'] = 'beginner'
                elif 'square' in difficulty_label:
                    trail_info['difficulty'] = 'intermediate'
                elif 'two diamond' in difficulty_label or 'double' in difficulty_label:
                    trail_info['difficulty'] = 'expert'
                elif 'diamond' in difficulty_label:
                    trail_info['difficulty'] = 'advanced'
            
            # Trail name from the heavy font td
            name_td = row.find('td', class_=lambda x: x and 'font-swiss-heavy' in x)
            if name_td:
                trail_info['name'] = name_td.get_text(strip=True)
            
            # Add area if we found one
            if current_area:
                trail_info['area'] = current_area
            
            # Only add if we got a name
            if trail_info.get('name'):
                data['trails'].append(trail_info)
        
        print(f"Extracted {len(data['trails'])} individual trails")
    except Exception as e:
        print(f"Warning: Could not extract trail details: {e}")
        data['trail_extraction_error'] = str(e)
    
    # Extract lift status
    try:
        # Find the "Lifts" heading
        lifts_heading = soup.find('h2', string=lambda x: x and 'Lifts' in x)
        
        if lifts_heading:
            # Find the table that follows the heading
            table = lifts_heading.find_next('table')
            
            if table:
                # Find all rows in this table
                lift_rows = table.find_all('tr', class_=lambda x: x and 'flex items-center' in x)
                
                for row in lift_rows:
                    lift_info = {}
                    
                    # Status from first SVG with aria-label
                    status_svg = row.find('svg', {'aria-label': True})
                    if status_svg:
                        aria_label = status_svg.get('aria-label', '')
                        if 'checkmark' in aria_label.lower():
                            lift_info['status'] = 'open'
                        elif 'x' in aria_label.lower():
                            lift_info['status'] = 'closed'
                        elif 'hold' in aria_label.lower():
                            lift_info['status'] = 'on_hold'
                    
                    # Lift name from any td with text
                    name_td = row.find('td', class_=lambda x: x and 'whitespace-nowrap' in x and 'font' in str(x))
                    if not name_td:
                        # Try finding any td with substantive text
                        for td in row.find_all('td'):
                            text = td.get_text(strip=True)
                            # Skip tds that only have times or are empty
                            if text and len(text) > 3 and not re.match(r'^\d+:\d+', text):
                                name_td = td
                                break
                    
                    if name_td:
                        lift_info['name'] = name_td.get_text(strip=True)
                    
                    # Only add if we got a name
                    if lift_info.get('name'):
                        data['lifts'].append(lift_info)
                
                print(f"Extracted {len(data['lifts'])} individual lifts")
            else:
                print("Warning: Could not find lifts table")
        else:
            print("Warning: Could not find Lifts heading")
    except Exception as e:
        print(f"Warning: Could not extract lift details: {e}")
    
    return data

def combine_and_save(**context):
    """
    Combine data from both pages and save to Bronze layer
    """
    ti = context['ti']
    
    # Pull data from both scraping tasks
    mountain_data = ti.xcom_pull(task_ids='scrape_mountain_report')
    trails_data = ti.xcom_pull(task_ids='scrape_trails_lifts')
    
    if not mountain_data or not trails_data:
        raise ValueError("Missing data from one or both scraping tasks")
    
    # Combine into single record
    combined = {
        'mountain': 'cannon',
        'scraped_at': datetime.now().isoformat(),
        'source_urls': [
            'https://www.cannonmt.com/mountain-report',
            'https://www.cannonmt.com/trails-lifts'
        ],
        **mountain_data,
        **trails_data
    }
    
    # Save to MinIO
    s3 = create_minio_client()
    
    date_str = datetime.now().strftime('%Y-%m-%d')
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    bucket_name = 'bronze-snow-reports'
    object_key = f'cannon/daily/{date_str}/{timestamp_str}.json'
    
    json_data = json.dumps(combined, indent=2)
    
    s3.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=json_data.encode('utf-8'),
        ContentType='application/json',
        Metadata={
            'mountain': 'cannon',
            'scrape_date': date_str,
        }
    )
    
    print(f"Saved to Bronze layer: s3://{bucket_name}/{object_key}")
    print(f"  File size: {len(json_data)} bytes")
    print(f"  Total trails: {len(combined.get('trails', []))}")
    print(f"  Total lifts: {len(combined.get('lifts', []))}")
    
    return object_key

# Define the DAG
with DAG(
    dag_id='scrape_cannon',
    default_args=default_args,
    description='Daily scrape of Cannon Mountain snow report (both pages)',
    schedule='0 12 * * *',  # Run daily at 12 PM UTC (7 AM EST)
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=['scraper', 'cannon', 'bronze'],
) as dag:
    
    scrape_report = PythonOperator(
        task_id='scrape_mountain_report',
        python_callable=scrape_mountain_report,
    )
    
    scrape_trails = PythonOperator(
        task_id='scrape_trails_lifts',
        python_callable=scrape_trails_lifts,
    )
    
    save_task = PythonOperator(
        task_id='combine_and_save',
        python_callable=combine_and_save,
    )
    
    # Both scraping tasks run in parallel, then combine
    [scrape_report, scrape_trails] >> save_task