"""
Cranmore Snow Report Scraper - VERSION 2.0
===========================================

NEW in v2.0:
    - Weather data extraction (temperature, wind speed/direction)
    - Lift capacity parsing (quad=4, triple=3, double=2)
    - Enhanced error handling and logging
    - More detailed summary metrics

Purpose: 
    Daily scraping of ALL available snow conditions from Cranmore
    - Structured metrics (lifts open, trails open, snowfall, weather)
    - Individual lift details (name, status, hours, capacity)
    - Individual trail details (name, status, difficulty, conditions, night skiing)
    - Terrain parks
    - Full narrative snow report

Architecture Decision:
    We store EVERYTHING in Bronze layer as-is, then transform/filter in Silver.
    Why? Because website structure might change, and we want the raw data preserved.

Data Quality Approach:
    - Graceful degradation: if one section fails, we still save what we got
    - Each extraction is wrapped in try/except with clear error messages
    - Missing data is explicitly marked as None (not empty string)
    
Schema Design:
    Bronze layer = exactly what the website shows (minimal transformation)
    Silver layer = standardized, validated, analytics-ready
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

def parse_lift_capacity(lift_name):
    """
    Extract lift capacity from lift name
    
    Common patterns:
    - "Quad" → 4 passengers
    - "Triple" → 3 passengers
    - "Double" → 2 passengers
    - "Gondola" → 4-8 (we'll use 6 as typical)
    - "Carpet" / "Surface" → 0 (not a chair)
    
    Returns:
        int or None: Capacity if determinable, None otherwise
    """
    name_lower = lift_name.lower()
    
    if 'gondola' in name_lower:
        return 6  # Typical gondola capacity
    elif 'quad' in name_lower:
        return 4
    elif 'triple' in name_lower:
        return 3
    elif 'double' in name_lower:
        return 2
    elif 'carpet' in name_lower or 'surface' in name_lower or 'rope' in name_lower:
        return 0  # Surface lift, not a chair
    elif 'chair' in name_lower:
        return 2  # Generic "chair" usually means double
    else:
        return None  # Unknown/can't determine

def scrape_cranmore():
    """
    Comprehensive scrape of Cranmore snow report (v2.0)
    
    Returns a nested JSON structure with:
        - metadata (mountain, timestamp, url, scraper version)
        - summary_metrics (open lifts, trails, snowfall totals)
        - weather (temperature, wind - NEW in v2.0)
        - lifts (array of individual lift objects with capacity)
        - trails (array of individual trail objects)
        - terrain_parks (array of terrain park objects)
        - narrative_report (full text report)
    """
    url = 'https://cranmore.com/snow-report'
    
    headers = {
        'User-Agent': 'PowderPredictor/2.0 (Educational Project; [email protected])'
    }

    print(f"Fetching snow report from {url}")
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Initialize data structure
    data = {
        'metadata': {
            'mountain': 'cranmore',
            'scraped_at': datetime.now().isoformat(),
            'source_url': url,
            'scraper_version': '2.0'
        },
        'summary_metrics': {},
        'weather': {},  # NEW in v2.0
        'lifts': [],
        'trails': [],
        'terrain_parks': [],
        'narrative_report': ''
    }

    # ========================================================================
    # SECTION 1: Extract Summary Metrics (datapoint divs)
    # ========================================================================
    print("\n[1/6] Extracting summary metrics...")
    try:
        datapoints = soup.find_all('div', class_='datapoint')
        
        for dp in datapoints:
            h3 = dp.find('h3')
            value = dp.find('span', class_='value')
            
            if h3 and value:
                label = h3.get_text(strip=True)
                val = value.get_text(strip=True)
                
                # Map website labels to our field names
                if 'Open Lifts' in label:
                    data['summary_metrics']['open_lifts'] = val
                elif 'Open Trails' in label:
                    data['summary_metrics']['open_trails'] = val
                elif 'Night Trails' in label:
                    data['summary_metrics']['night_trails'] = val
                elif 'Base Depth' in label:
                    data['summary_metrics']['base_depth'] = val
                elif '48 Hour' in label or '48hr' in label.lower():
                    data['summary_metrics']['snowfall_48hr'] = val
                elif '7 Day' in label:
                    data['summary_metrics']['snowfall_7day'] = val
                elif 'Season Total' in label:
                    data['summary_metrics']['season_total'] = val
        
        print(f"   ✓ Extracted {len(data['summary_metrics'])} summary metrics")
    except Exception as e:
        print(f"   ✗ Warning: Could not extract summary metrics: {e}")
        data['summary_metrics']['extraction_error'] = str(e)

    # ========================================================================
    # SECTION 2: Extract Weather Data (NEW in v2.0)
    # ========================================================================
    # Weather might appear in:
    # - Dedicated weather section
    # - Datapoint divs (like temperature, wind)
    # - Narrative text (as fallback)
    
    print("\n[2/6] Extracting weather data...")
    try:
        # Look for temperature in datapoints
        for dp in soup.find_all('div', class_='datapoint'):
            h3 = dp.find('h3')
            value = dp.find('span', class_='value')
            
            if h3 and value:
                label = h3.get_text(strip=True).lower()
                val = value.get_text(strip=True)
                
                if 'temperature' in label or 'temp' in label:
                    # Might be "Temperature" or "Summit Temp" or "Base Temp"
                    if 'summit' in label:
                        data['weather']['temperature_summit'] = val
                    elif 'base' in label:
                        data['weather']['temperature_base'] = val
                    else:
                        data['weather']['temperature'] = val
                
                elif 'wind' in label:
                    if 'gust' in label:
                        data['weather']['wind_gust'] = val
                    elif 'direction' in label:
                        data['weather']['wind_direction'] = val
                    else:
                        data['weather']['wind_speed'] = val
                
                elif 'weather' in label or 'conditions' in label:
                    data['weather']['conditions'] = val
        
        # Also check for weather in narrative (as fallback)
        # This is a backup if weather isn't in structured datapoints
        # We'll parse the narrative text for weather mentions
        
        if data['weather']:
            print(f"   ✓ Extracted {len(data['weather'])} weather fields")
        else:
            print(f"   ℹ No structured weather data found (Cranmore may not report it)")
            
    except Exception as e:
        print(f"   ✗ Warning: Could not extract weather data: {e}")

    # ========================================================================
    # SECTION 3: Extract Narrative Report
    # ========================================================================
    print("\n[3/6] Extracting narrative report...")
    try:
        report_elem = soup.find(string=lambda text: text and 'Report:' in text)
        
        if report_elem:
            report_parts = []
            
            # Walk through siblings after "Report:"
            for sibling in report_elem.find_next_siblings():
                # Stop when we hit structured data (the datapoint divs)
                if sibling.name == 'div':
                    break
                
                # Only capture paragraph tags
                if sibling.name == 'p':
                    text = sibling.get_text(strip=True)
                    if text and text != 'Report:':
                        report_parts.append(text)
            
            data['narrative_report'] = '\n\n'.join(report_parts)
        
        # Also grab "Last Updated" timestamp
        last_updated = soup.find(string=lambda text: text and 'Last Updated:' in text)
        if last_updated:
            data['metadata']['last_updated'] = last_updated.strip()
        
        print(f"   ✓ Extracted narrative ({len(data['narrative_report'])} chars)")
    except Exception as e:
        print(f"   ✗ Warning: Could not extract narrative: {e}")
        data['narrative_report'] = ''

    # ========================================================================
    # SECTION 4: Extract Individual Lifts (with capacity - NEW in v2.0)
    # ========================================================================
    print("\n[4/6] Extracting lift details...")
    try:
        lifts_header = soup.find('h2', string='Lifts')
        
        if lifts_header:
            datatable = lifts_header.find_next_sibling('div', class_='datatable')
            
            if datatable:
                rows = datatable.find_all('div', class_='row')
                
                for row in rows:
                    cols = row.find_all('div', class_='col')
                    
                    # Skip header rows and section dividers
                    if len(cols) >= 2:
                        if row.find('h3'):
                            continue
                        
                        name = cols[0].get_text(strip=True)
                        status_elem = cols[1].find('span', class_='status')
                        status = status_elem.get_text(strip=True) if status_elem else 'unknown'
                        
                        # Hours are in the third column (if present)
                        hours = cols[2].get_text(strip=True) if len(cols) >= 3 else None
                        
                        # NEW in v2.0: Parse lift capacity from name
                        capacity = parse_lift_capacity(name)
                        
                        if name:
                            lift = {
                                'name': name,
                                'status': status,
                                'hours': hours,
                                'capacity': capacity  # NEW field
                            }
                            data['lifts'].append(lift)
        
        print(f"   ✓ Extracted {len(data['lifts'])} lifts (with capacity)")
    except Exception as e:
        print(f"   ✗ Warning: Could not extract lifts: {e}")

    # ========================================================================
    # SECTION 5: Extract Individual Trails
    # ========================================================================
    print("\n[5/6] Extracting trail details...")
    try:
        trails_header = soup.find('h2', string='Trails')
        
        if trails_header:
            datatable = trails_header.find_next_sibling('div', class_='datatable')
            
            if datatable:
                rows = datatable.find_all('div', class_='row')
                
                for row in rows:
                    cols = row.find_all('div', class_='col')
                    
                    # Skip category headers
                    if row.find('h3'):
                        continue
                    
                    if len(cols) >= 4:
                        name = cols[0].get_text(strip=True)
                        
                        # Status
                        status_elem = cols[1].find('span', class_='status')
                        status = status_elem.get_text(strip=True) if status_elem else 'unknown'
                        
                        # Night skiing (check for moon SVG)
                        night_skiing = bool(cols[2].find('svg', class_='night'))
                        
                        # Difficulty (SVG with alt attribute)
                        difficulty = None
                        difficulty_svg = cols[3].find('svg', class_='difficulty')
                        if difficulty_svg:
                            difficulty = difficulty_svg.get('alt', 'unknown')
                        
                        # Conditions (text in last column)
                        conditions = cols[4].get_text(strip=True) if len(cols) >= 5 else None
                        
                        if name:
                            trail = {
                                'name': name,
                                'status': status,
                                'night_skiing': night_skiing,
                                'difficulty': difficulty,
                                'conditions': conditions
                            }
                            data['trails'].append(trail)
        
        print(f"   ✓ Extracted {len(data['trails'])} trails")
    except Exception as e:
        print(f"   ✗ Warning: Could not extract trails: {e}")

    # ========================================================================
    # SECTION 6: Separate Terrain Parks from Trails
    # ========================================================================
    print("\n[6/6] Separating terrain parks...")
    try:
        # Filter trails for terrain parks (difficulty == "park")
        data['terrain_parks'] = [
            trail for trail in data['trails'] 
            if trail.get('difficulty') == 'park'
        ]
        
        # Remove terrain parks from main trails list
        data['trails'] = [
            trail for trail in data['trails'] 
            if trail.get('difficulty') != 'park'
        ]
        
        print(f"   ✓ Found {len(data['terrain_parks'])} terrain parks")
    except Exception as e:
        print(f"   ✗ Warning: Could not separate terrain parks: {e}")

    # ========================================================================
    # Final summary
    # ========================================================================
    print("\n" + "="*60)
    print("SCRAPING COMPLETE - v2.0")
    print("="*60)
    print(f"Summary Metrics: {len(data['summary_metrics'])} fields")
    print(f"Weather Data: {len(data['weather'])} fields")
    print(f"Lifts: {len(data['lifts'])} items (with capacity)")
    print(f"Trails: {len(data['trails'])} items")
    print(f"Terrain Parks: {len(data['terrain_parks'])} items")
    print(f"Narrative: {len(data['narrative_report'])} chars")
    print("="*60 + "\n")
    
    return data

def save_to_bronze(**context):
    """
    Save comprehensive scraped data to Bronze layer in MinIO
    """
    ti = context['ti']
    data = ti.xcom_pull(task_ids='scrape_snow_report')
    
    if not data:
        raise ValueError("No data received from scraping task")
    
    s3 = create_minio_client()
    
    # Create filename with date and timestamp
    date_str = datetime.now().strftime('%Y-%m-%d')
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    bucket_name = 'bronze-snow-reports'
    object_key = f'cranmore/daily/{date_str}/{timestamp_str}.json'
    
    # Convert to JSON with pretty printing
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
            'scraper_version': data['metadata']['scraper_version']
        }
    )
    
    print(f"✓ Saved to Bronze layer: s3://{bucket_name}/{object_key}")
    print(f"✓ File size: {len(json_data)} bytes")
    
    return object_key

# Define the DAG
with DAG(
    dag_id='scrape_cranmore_v2',
    default_args=default_args,
    description='Daily scrape of Cranmore snow report (v2.0 with weather)',
    schedule='0 12 * * *',  # Run daily at 12 PM UTC (7 AM EST)
    start_date=datetime(2026, 1, 19),
    catchup=False,
    tags=['scraper', 'cranmore', 'bronze', 'v2'],
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