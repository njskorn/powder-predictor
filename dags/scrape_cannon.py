"""
Cannon Mountain Snow Report Scraper - VERSION 3.0
=================================================

FIXED in v3.0:
    - Proper HTML structure navigation (no more regex on text)
    - Area detection using <div class="mb-7"> and <h3> tags
    - Difficulty extraction from SVG aria-labels
    - No more lift/trail contamination
    - No more "Variable conditions" as trail names
    - Proper temperature extraction (BASE low/high)
    - Trail-specific table parsing (not all <tr> on page)

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
from airflow.models import Variable
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
    Comprehensive scrape of Cannon Mountain (v3.0 - MAJOR FIX)
    
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
        'User-Agent': 'PowderPredictor/3.0 (Educational Project; [email protected])'
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
            'scraper_version': '3.0'
        },
        'summary_metrics': {
            'mountain_report_page': {},  # Numbers from page 1
            'trails_lifts_page': {}      # Numbers from page 2 (may differ!)
        },
        'weather': {
            'base': {},
            'summit': {}
        },
        'narrative_report': '',
        'lifts': [],
        'trails': [],
        'glades': []
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
        
        # Open trails
        trails_match = re.search(r'(\d+)\s*OPEN TRAILS', page1_text)
        if trails_match:
            data['summary_metrics']['mountain_report_page']['open_trails'] = trails_match.group(1)
        
        lifts_match = re.search(r'(\d+)\s*OPEN LIFTS', page1_text)
        if lifts_match:
            data['summary_metrics']['mountain_report_page']['open_lifts'] = lifts_match.group(1)
        
        # New snowfall
        new_snow_match = re.search(r'(\d+)"?\s*NEW SNOW', page1_text)
        if new_snow_match:
            data['summary_metrics']['mountain_report_page']['snowfall_new'] = new_snow_match.group(1) + '"'
        
        # Snowfall to date
        snowfall_match = re.search(r'(\d+)"?\s*(?:SNOW\s+)?TO DATE', page1_text)
        if snowfall_match:
            data['summary_metrics']['mountain_report_page']['snowfall_to_date'] = snowfall_match.group(1) + '"'
        
        print(f"   ✓ Extracted {len(data['summary_metrics']['mountain_report_page'])} metrics")
        
        # Extract Weather - BASE vs SUMMIT (v2.1 patterns)
        print("\n[2/4] Extracting BASE and SUMMIT weather...")
        
        temp_low_match = re.search(r'LOW\s*(\d+)º', page1_text)
        temp_high_match = re.search(r'HIGH\s*(\d+)º', page1_text)
        
        if temp_low_match:
            data['weather']['base']['temperature_low'] = f"{temp_low_match.group(1)}°F"
        if temp_high_match:
            data['weather']['base']['temperature_high'] = f"{temp_high_match.group(1)}°F"
        
        # Wind - BASE and SUMMIT
        wind_base_match = re.search(r'WIND\s*BASE\s*([NESW/]+),\s*([\d\-]+\s*mph)', page1_text)
        if wind_base_match:
            data['weather']['base']['wind_direction'] = wind_base_match.group(1)
            data['weather']['base']['wind_speed'] = wind_base_match.group(2)
        
        wind_summit_match = re.search(r'SUMMIT\s*([NESW/]+),\s*([\d\-]+\s*mph)', page1_text)
        if wind_summit_match:
            data['weather']['summit']['wind_direction'] = wind_summit_match.group(1)
            data['weather']['summit']['wind_speed'] = wind_summit_match.group(2)
        
        # Overall conditions
        conditions_match = re.search(r'(Increasing Clouds|Light Snow|Heavy Snow|Clear|Partly Cloudy|Cloudy|Snow Showers|Mostly Cloudy)', page1_text)
        if conditions_match:
            data['weather']['conditions'] = conditions_match.group(1)
        
        print(f"   ✓ BASE: {len(data['weather']['base'])} fields, SUMMIT: {len(data['weather']['summit'])} fields")
        
        # Extract Lifts from page 1 - v2.1 approach (was working)
        print("\n[3/4] Extracting lifts from Mountain Report...")

        # Look for lifts table (v2.1 method that worked)
        lifts_section = soup1.find(string=re.compile(r'Lifts'))
        if lifts_section:
            table = lifts_section.find_parent().find_next('table')
            if table:
                rows = table.find_all('tr')
                
                for row in rows:
                    try:
                        # Get status from SVG circle fill color
                        status = 'unknown'
                        svg_circle = row.find('circle')
                        if svg_circle:
                            fill = svg_circle.get('fill', '').upper()
                            if '27D94E' in fill:  # Green
                                status = 'open'
                            elif 'B52025' in fill:  # Red
                                status = 'closed'
                            elif 'FCDA00' in fill:  # Yellow
                                status = 'on hold'
                        
                        # Find lift name in div with 'uppercase' class
                        name_div = row.find('div', class_=lambda c: c and 'uppercase' in c if c else False)
                        if name_div:
                            name = name_div.get_text(strip=True)
                            
                            # Find hours
                            hours = None
                            time_divs = row.find_all('div')
                            for div in time_divs:
                                text = div.get_text(strip=True)
                                if ('a.m.' in text or 'p.m.' in text) and len(text) < 30:
                                    hours = text
                                    break
                            
                            if name:
                                data['lifts'].append({
                                    'name': name,
                                    'status': status,
                                    'hours': hours
                                })
                    except:
                        continue

        print(f"Extracted {len(data['lifts'])} lifts")

        parse_text = soup1.find('div', class_='parse-text')
        if parse_text:
            # Get all text, preserving line breaks
            data['narrative_report'] = parse_text.get_text(separator='\n', strip=True)
            # Count paragraphs to verify we got content
            para_count = len(parse_text.find_all('p'))
            print(f"Extracted narrative report ({para_count} paragraphs, {len(data['narrative_report'])} chars)")
        else:
            print("Warning: Could not find narrative snow report (.parse-text)")

        
    except Exception as e:
        print(f"   ✗ ERROR on page 1: {e}")
        import traceback
        traceback.print_exc()
        data['summary_metrics']['mountain_report_page']['scraping_error'] = str(e)

    # ========================================================================
    # PAGE 2: Trails & Lifts
    # ========================================================================
    
    print("\n" + "="*60)
    print("SCRAPING PAGE 2: Trails & Lifts (FIXED HTML PARSING)")
    print("="*60)
    
    try:
        print(f"\nFetching {trails_lifts_url}")
        response2 = requests.get(trails_lifts_url, headers=headers, timeout=30)
        response2.raise_for_status()
        soup2 = BeautifulSoup(response2.content, 'html.parser')
        page2_text = soup2.get_text()
        
        # Extract Summary from page 2
        print("\n[4/7] Extracting summary from Trails & Lifts...")
        
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
        
        # Extract Trails by Area - v3.0 FIX: Proper HTML structure
        print("\n[5/7] Extracting trails by area (v3.0 method)...")
        
        # Find all area sections (div with mb-7 class containing h3 and table)
        area_sections = soup2.find_all('div', class_='mb-7')
        
        for section in area_sections:
            # Get area name from h3
            h3 = section.find('h3', class_='uppercase')
            if not h3:
                continue
            
            area_name = h3.get_text(strip=True)
            
            # Skip if this is the Glades section (handled separately)
            if 'glade' in area_name.lower():
                continue
            
            print(f"   ℹ Processing area: {area_name}")
            
            # Find table in this section
            table = section.find('table')
            if not table:
                continue
            
            # Find all trail rows
            rows = table.find_all('tr')
            
            for row in rows:
                tds = row.find_all('td')
                
                # Need at least 4 columns (status, grooming, difficulty, name)
                if len(tds) < 4:
                    continue
                
                # Column 0: Status (SVG circle color)
                status = 'unknown'
                status_svg = tds[0].find('circle')
                if status_svg:
                    fill = status_svg.get('fill', '').upper()
                    if '27D94E' in fill:  # Green
                        status = 'open'
                    elif 'B52025' in fill:  # Red
                        status = 'closed'
                    elif 'FCDA00' in fill:  # Yellow
                        status = 'on hold'
                
                # Column 1: Grooming (presence of groomer SVG)
                groomed = False
                groomer_svg = tds[1].find('svg', attrs={'aria-label': lambda x: x and 'groomer' in x.lower() if x else False})
                if groomer_svg:
                    groomed = True
                
                # Column 2: Difficulty (SVG aria-label)
                difficulty = None
                diff_svg = tds[2].find('svg')
                if diff_svg:
                    aria_label = diff_svg.get('aria-label', '').lower()
                    if 'green' in aria_label:
                        difficulty = 'green'
                    elif 'blue' in aria_label:
                        difficulty = 'blue'
                    elif 'black' in aria_label or 'diamond' in aria_label:
                        difficulty = 'black'
                
                # Column 3: Trail name
                trail_name = tds[3].get_text(strip=True)
                
                # Skip empty names
                if not trail_name or len(trail_name) < 2:
                    continue
                
                # Column 4 (optional): Conditions/warnings
                warning = None
                if len(tds) > 4:
                    cond_text = tds[4].get_text(strip=True)
                    if cond_text and cond_text.lower() != 'variable conditions':
                        warning = cond_text
                
                # Add trail
                trail = {
                    'name': trail_name,
                    'status': status,
                    'area': area_name,
                    'warning': warning,
                    'difficulty': difficulty,
                    'conditions': 'groomed' if groomed else None,
                    'night_skiing': False
                }
                
                data['trails'].append(trail)
        
        print(f"   ✓ Extracted {len(data['trails'])} trails across all areas")
        
        # Extract Glades - v3.0: Separate section
        print("\n[6/7] Extracting glades...")
        
        # Find Glades section
        glades_header = soup2.find('h3', string=re.compile(r'Glades', re.I))
        if glades_header:
            glades_section = glades_header.find_parent('div', class_='mb-7')
            if glades_section:
                table = glades_section.find('table')
                if table:
                    rows = table.find_all('tr')
                    
                    for row in rows:
                        tds = row.find_all('td')
                        if len(tds) < 4:
                            continue
                        
                        # Same column structure as trails
                        status = 'unknown'
                        status_svg = tds[0].find('circle')
                        if status_svg:
                            fill = status_svg.get('fill', '').upper()
                            if '27D94E' in fill:
                                status = 'open'
                            elif 'B52025' in fill:
                                status = 'closed'
                        
                        difficulty = None
                        diff_svg = tds[2].find('svg')
                        if diff_svg:
                            aria_label = diff_svg.get('aria-label', '').lower()
                            if 'green' in aria_label:
                                difficulty = 'green'
                            elif 'blue' in aria_label:
                                difficulty = 'blue'
                            elif 'black' in aria_label or 'diamond' in aria_label:
                                difficulty = 'black'
                        
                        glade_name = tds[3].get_text(strip=True)
                        
                        # Get notes from column 4 (italic text)
                        notes = None
                        if len(tds) > 4:
                            notes_text = tds[4].get_text(strip=True)
                            if notes_text:
                                notes = notes_text
                        
                        if glade_name and len(glade_name) > 2:
                            data['glades'].append({
                                'name': glade_name,
                                'status': status,
                                'area': 'Glades',
                                'difficulty': difficulty,
                                'conditions': notes
                            })
        
        print(f"   ✓ Extracted {len(data['glades'])} glades")
        
        # Statistics
        print("\n[7/7] Final statistics...")
        
        # Count by area
        areas = {}
        for trail in data['trails']:
            area = trail.get('area', 'Unknown')
            areas[area] = areas.get(area, 0) + 1
        
        print(f"   ℹ Areas: {', '.join(f'{area} ({count})' for area, count in sorted(areas.items()))}")
        
        # Count by difficulty
        difficulties = {}
        for trail in data['trails']:
            diff = trail.get('difficulty') or 'None'
            difficulties[diff] = difficulties.get(diff, 0) + 1
        
        print(f"   ℹ Difficulties: {', '.join(f'{diff} ({count})' for diff, count in sorted(difficulties.items()))}")
        
    except Exception as e:
        print(f"   ✗ ERROR on page 2: {e}")
        import traceback
        traceback.print_exc()
        data['summary_metrics']['trails_lifts_page']['scraping_error'] = str(e)

    print("\n" + "="*60)
    print("SCRAPING COMPLETE - CANNON v3.0")
    print("="*60)
    print(f"Lifts: {len(data['lifts'])}")
    print(f"Trails: {len(data['trails'])}")
    print(f"Glades: {len(data['glades'])}")
    print("="*60)
    
    return data

def save_to_bronze(**context):
    """Save to Bronze layer in MinIO"""
    ti = context['ti']
    data = ti.xcom_pull(task_ids='scrape_snow_report')
    
    if not data:
        raise ValueError("No data received from scraper")
    
    s3 = create_minio_client()
    
    date_str = datetime.now().strftime('%Y-%m-%d')
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    bucket_name = 'bronze-snow-reports'
    object_key = f'cannon/daily/{date_str}/{timestamp_str}.json'
    
    # Ensure bucket exists
    try:
        s3.head_bucket(Bucket=bucket_name)
    except:
        s3.create_bucket(Bucket=bucket_name)
        print(f"✓ Created bucket: {bucket_name}")
    
    # Save to MinIO
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
    
    print(f"✓ Saved to MinIO: s3://{bucket_name}/{object_key}")
    return object_key

def ping_healthcheck(**context):
    """
    Ping Healthchecks.io after successful scrape
    
    This is called only if the scrape + save succeeded
    """
    try:
        # Get the ping URL from Airflow Variables
        ping_url = Variable.get('healthchecks_cannon_url')
        
        # Ping Healthchecks.io
        response = requests.get(ping_url, timeout=10)
        
        if response.status_code == 200:
            print("Successfully pinged Healthchecks.io")
        else:
            print(f"Healthchecks ping returned status {response.status_code}")
            
    except Exception as e:
        # Don't fail the DAG if healthcheck ping fails
        # This is monitoring, not critical path
        print(f"Could not ping Healthchecks.io: {e}")

with DAG(
    dag_id='scrape_cannon_v3',
    default_args=default_args,
    description='Daily Cannon scrape (v3.0 - proper HTML structure parsing)',
    schedule='0 12 * * *',
    start_date=datetime(2026, 1, 26),
    catchup=False,
    tags=['scraper', 'cannon', 'bronze', 'v3'],
) as dag:
    
    # Task 1: Scrape the website
    scrape_task = PythonOperator(
        task_id='scrape_snow_report',
        python_callable=scrape_cannon,
    )
    
    # Task 2: Save to Bronze layer
    save_task = PythonOperator(
        task_id='save_to_bronze',
        python_callable=save_to_bronze,
    )

    # Task 3: healthcheck
    healthcheck_task = PythonOperator(
        task_id='ping_healthcheck',
        python_callable=ping_healthcheck,
    )
    
    # Define dependency
    scrape_task >> save_task >> healthcheck_task