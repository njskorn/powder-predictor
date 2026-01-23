"""
Cannon Mountain Snow Report Scraper - VERSION 2.0
==================================================

Built from actual HTML structure analysis on 2026-01-22

Cannon Mountain Specific Features:
    - TWO PAGES to scrape (mountain-report + trails-lifts)
    - BASE vs SUMMIT wind data (critical for Cannon!)
    - Trail organization by AREA (5 areas + glades)
    - Glades SEPARATE from trails
    - Trail warnings/notes (e.g., "Use Caution: Thin Cover")
    - Very detailed narrative reports
    - Marketing: "TALLEST vertical drop in NH!" (likely mentioned)
    - Potential data discrepancies between pages (we capture both)
    
NEW in v2.0:
    - Dual-page scraping strategy
    - Base vs Summit weather (especially wind!)
    - Trail warnings field
    - Acreage and mileage metrics
    - No lift capacity in Bronze (moved to Silver layer)
    - Area field for geographic organization

Purpose: 
    Daily scraping of ALL available snow conditions from Cannon Mountain
    - Data from BOTH pages (may have discrepancies - we track them)
    - Detailed metrics (lifts, trails, glades, acreage, mileage, snowfall)
    - BASE and SUMMIT weather (wind is critical!)
    - Individual lift details (name, status, hours)
    - Individual trail details WITH AREA and WARNINGS
    - Individual glade details WITH AREA
    - Full narrative snow report

Architecture:
    Bronze layer = exactly what both pages show (capture discrepancies)
    Silver layer = reconcile, validate, standardize
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

def scrape_cannon():
    """
    Comprehensive scrape of Cannon Mountain (v2.0)
    
    Scrapes TWO pages:
    1. Mountain Report - summary metrics, weather, narrative
    2. Trails & Lifts - detailed trail/lift listings with areas
    
    Returns nested JSON with:
        - metadata (mountain, timestamp, urls, scraper version)
        - summary_metrics (from BOTH pages - may differ!)
        - weather (BASE and SUMMIT conditions)
        - lifts (array of lift objects)
        - trails (array with AREA and WARNING fields)
        - glades (array with AREA - SEPARATE from trails!)
        - terrain_parks (array)
        - narrative_report (very detailed at Cannon)
    """
    
    # URLs for both pages
    mountain_report_url = 'https://www.cannonmt.com/mountain-report'
    trails_lifts_url = 'https://www.cannonmt.com/trails-lifts'
    
    headers = {
        'User-Agent': 'PowderPredictor/2.0 (Educational Project; nettle@example.com)'
    }

    print(f"Fetching Cannon Mountain data from 2 pages...")
    
    # Initialize data structure
    data = {
        'metadata': {
            'mountain': 'cannon',
            'scraped_at': datetime.now().isoformat(),
            'source_urls': {
                'mountain_report': mountain_report_url,
                'trails_lifts': trails_lifts_url
            },
            'scraper_version': '2.0'
        },
        'summary_metrics': {
            'mountain_report_page': {},  # Numbers from page 1
            'trails_lifts_page': {}      # Numbers from page 2 (may differ!)
        },
        'weather': {
            'base': {},
            'summit': {}
        },
        'lifts': [],
        'trails': [],
        'glades': [],
        'terrain_parks': [],
        'narrative_report': ''
    }

    # ========================================================================
    # PAGE 1: Mountain Report
    # ========================================================================
    
    print("\n" + "="*60)
    print("SCRAPING PAGE 1: Mountain Report")
    print("="*60)
    
    try:
        print(f"\nFetching {mountain_report_url}")
        response1 = requests.get(mountain_report_url, headers=headers, timeout=30)
        response1.raise_for_status()
        soup1 = BeautifulSoup(response1.content, 'html.parser')
        page1_text = soup1.get_text()
        
        # Extract Summary Metrics
        print("\n[1/4] Extracting summary metrics from Mountain Report...")
        
        snowfall_match = re.search(r'(\d+)"\s*SNOWFALL TO DATE', page1_text)
        if snowfall_match:
            data['summary_metrics']['mountain_report_page']['snowfall_to_date'] = f'{snowfall_match.group(1)}"'
        
        trails_match = re.search(r'(\d+)\s*OPEN TRAILS', page1_text)
        if trails_match:
            data['summary_metrics']['mountain_report_page']['open_trails'] = trails_match.group(1)
        
        lifts_match = re.search(r'(\d+)\s*OPEN LIFTS', page1_text)
        if lifts_match:
            data['summary_metrics']['mountain_report_page']['open_lifts'] = lifts_match.group(1)
        
        new_snow_match = re.search(r'(\d+)"\s*NEW SNOWFALL', page1_text)
        if new_snow_match:
            data['summary_metrics']['mountain_report_page']['snowfall_new'] = f'{new_snow_match.group(1)}"'
        
        primary_match = re.search(r'PRIMARY SURFACE\s*([^\n]+)', page1_text)
        if primary_match:
            data['summary_metrics']['mountain_report_page']['primary_surface'] = primary_match.group(1).strip()
        
        secondary_match = re.search(r'SECONDARY SURFACE\s*([^\n]+)', page1_text)
        if secondary_match:
            data['summary_metrics']['mountain_report_page']['secondary_surface'] = secondary_match.group(1).strip()
        
        print(f"   ✓ Extracted {len(data['summary_metrics']['mountain_report_page'])} metrics")
        
        # Extract Weather - BASE vs SUMMIT
        print("\n[2/4] Extracting BASE and SUMMIT weather...")
        
        temp_low_match = re.search(r'LOW\s*(\d+)º', page1_text)
        temp_high_match = re.search(r'HIGH\s*(\d+)º', page1_text)
        
        if temp_low_match:
            data['weather']['base']['temperature_low'] = f"{temp_low_match.group(1)}°F"
        if temp_high_match:
            data['weather']['base']['temperature_high'] = f"{temp_high_match.group(1)}°F"
        
        wind_base_match = re.search(r'BASE\s*([NESW/]+),\s*([\d-]+\s*mph)', page1_text)
        if wind_base_match:
            data['weather']['base']['wind_direction'] = wind_base_match.group(1)
            data['weather']['base']['wind_speed'] = wind_base_match.group(2)
        
        wind_summit_match = re.search(r'SUMMIT\s*([NESW/]+),\s*([\d-]+\s*mph)', page1_text)
        if wind_summit_match:
            data['weather']['summit']['wind_direction'] = wind_summit_match.group(1)
            data['weather']['summit']['wind_speed'] = wind_summit_match.group(2)
        
        conditions_match = re.search(r'(Increasing Clouds|Light Snow|Heavy Snow|Clear|Partly Cloudy|Cloudy|Snow Showers)', page1_text)
        if conditions_match:
            data['weather']['conditions'] = conditions_match.group(1)
        
        print(f"   ✓ BASE: {len(data['weather']['base'])} fields, SUMMIT: {len(data['weather']['summit'])} fields")
        if data['weather']['summit'].get('wind_speed'):
            print(f"   ℹ SUMMIT WIND: {data['weather']['summit']['wind_speed']}")
        
        # Extract Narrative
        print("\n[3/4] Extracting narrative...")
        
        quote_elem = soup1.find(string=re.compile(r'"In skiing|Good afternoon|Good morning'))
        if quote_elem:
            parent = quote_elem.find_parent()
            narrative_parts = [quote_elem.strip()]
            
            for sibling in parent.find_next_siblings():
                text = sibling.get_text(strip=True)
                if text and len(text) > 50:
                    narrative_parts.append(text)
                    if len(narrative_parts) >= 10:
                        break
            
            data['narrative_report'] = '\n\n'.join(narrative_parts)
        
        updated_match = re.search(r'Last Updated:\s*([^\n]+)', page1_text)
        if updated_match:
            data['metadata']['last_updated'] = updated_match.group(1).strip()
        
        print(f"   ✓ Narrative: {len(data['narrative_report'])} chars")
        
        # Extract Lifts
        print("\n[4/4] Extracting lifts...")
        
        lifts_section = soup1.find(string=re.compile(r'Lifts'))
        if lifts_section:
            table = lifts_section.find_next('table')
            if table:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 1:
                        text = cells[0].get_text(strip=True)
                        parts = text.split('  ')
                        if len(parts) >= 1:
                            name = parts[0].strip()
                            hours = parts[1].strip() if len(parts) > 1 else None
                            status = 'closed'
                            
                            if name and any(kw in name.lower() for kw in ['quad', 'triple', 'double', 'tram', 't-bar', 'tow', 'carpet']):
                                data['lifts'].append({
                                    'name': name,
                                    'status': status,
                                    'hours': hours
                                })
        
        print(f"   ✓ Extracted {len(data['lifts'])} lifts")
        
    except Exception as e:
        print(f"   ✗ ERROR: {e}")
        data['summary_metrics']['mountain_report_page']['scraping_error'] = str(e)

    # ========================================================================
    # PAGE 2: Trails & Lifts
    # ========================================================================
    
    print("\n" + "="*60)
    print("SCRAPING PAGE 2: Trails & Lifts")
    print("="*60)
    
    try:
        print(f"\nFetching {trails_lifts_url}")
        response2 = requests.get(trails_lifts_url, headers=headers, timeout=30)
        response2.raise_for_status()
        soup2 = BeautifulSoup(response2.content, 'html.parser')
        page2_text = soup2.get_text()
        
        # Extract Summary
        print("\n[5/8] Extracting summary from Trails & Lifts...")
        
        trails_count = re.search(r'(\d+)\s*TRAILS', page2_text)
        if trails_count:
            data['summary_metrics']['trails_lifts_page']['trails_count'] = trails_count.group(1)
        
        lifts_count = re.search(r'(\d+)\s*LIFTS', page2_text)
        if lifts_count:
            data['summary_metrics']['trails_lifts_page']['lifts_count'] = lifts_count.group(1)
        
        acres_match = re.search(r'(\d+)\s*ACRES', page2_text)
        if acres_match:
            data['summary_metrics']['trails_lifts_page']['acres'] = acres_match.group(1)
        
        miles_match = re.search(r'([\d.]+)\s*MILES', page2_text)
        if miles_match:
            data['summary_metrics']['trails_lifts_page']['miles'] = miles_match.group(1)
        
        print(f"   ✓ {len(data['summary_metrics']['trails_lifts_page'])} fields (may differ from page 1)")
        
        # Extract Trails by Area
        print("\n[6/8] Extracting trails by area...")
        
        area_headers = ['Upper Mountain', 'Tuckerbrook Family Area', 'Mid-Mountain', 'Front5', 'Mittersill']
        current_area = None
        
        for elem in soup2.find_all(['h3', 'td']):
            text = elem.get_text(strip=True)
            
            if text in area_headers:
                current_area = text
                print(f"   ℹ Area: {current_area}")
                continue
            
            if current_area and len(text) > 2 and text.isupper():
                warning = None
                if 'Use Caution' in text or 'Thin Cover' in text or 'Race Training' in text:
                    parts = text.split('|')
                    name = parts[0].strip() if len(parts) > 1 else text
                    warning = parts[1].strip() if len(parts) > 1 else None
                else:
                    name = text
                
                if len(name) > 2 and not name.isdigit():
                    data['trails'].append({
                        'name': name,
                        'status': 'unknown',
                        'area': current_area,
                        'warning': warning,
                        'difficulty': None,
                        'conditions': None,
                        'night_skiing': False
                    })
        
        print(f"   ✓ {len(data['trails'])} trails")
        
        # Extract Glades
        print("\n[7/8] Extracting glades...")
        
        glades_header = soup2.find('h3', string='Glades')
        if glades_header:
            for elem in glades_header.find_next_siblings(['td']):
                text = elem.get_text(strip=True)
                if text and len(text) > 2 and text.isupper():
                    data['glades'].append({
                        'name': text,
                        'status': 'unknown',
                        'area': 'Glades',
                        'difficulty': None,
                        'conditions': None
                    })
        
        print(f"   ✓ {len(data['glades'])} glades")
        
    except Exception as e:
        print(f"   ✗ ERROR: {e}")
        data['summary_metrics']['trails_lifts_page']['scraping_error'] = str(e)

    print("\n" + "="*60)
    print("SCRAPING COMPLETE - CANNON v2.0")
    print("="*60)
    
    return data

def save_to_bronze(**context):
    """Save to Bronze layer"""
    ti = context['ti']
    data = ti.xcom_pull(task_ids='scrape_snow_report')
    
    if not data:
        raise ValueError("No data received")
    
    s3 = create_minio_client()
    
    date_str = datetime.now().strftime('%Y-%m-%d')
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    bucket_name = 'bronze-snow-reports'
    object_key = f'cannon/daily/{date_str}/{timestamp_str}.json'
    
    json_data = json.dumps(data, indent=2)
    
    s3.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=json_data.encode('utf-8'),
        ContentType='application/json',
        Metadata={
            'mountain': 'cannon',
            'scrape_date': date_str,
            'scraper_version': data['metadata']['scraper_version']
        }
    )
    
    print(f"✓ Saved: s3://{bucket_name}/{object_key}")
    return object_key

with DAG(
    dag_id='scrape_cannon_v2',
    default_args=default_args,
    description='Daily Cannon scrape (v2.0 - dual page)',
    schedule='0 12 * * *',
    start_date=datetime(2026, 1, 22),
    catchup=False,
    tags=['scraper', 'cannon', 'bronze', 'v2'],
) as dag:
    
    scrape_task = PythonOperator(
        task_id='scrape_snow_report',
        python_callable=scrape_cannon,
    )
    
    save_task = PythonOperator(
        task_id='save_to_bronze',
        python_callable=save_to_bronze,
    )
    
    scrape_task >> save_task