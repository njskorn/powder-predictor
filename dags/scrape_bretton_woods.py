"""
Bretton Woods Snow Report Scraper - VERSION 2.1 (FIXED)
========================================================

FIXED in v2.1:
    - Uses actual HTML structure instead of regex text parsing
    - Lifts: Navigate accordion structure properly
    - Trails: Parse from accordion sections with areas
    - Glades: Parse from separate accordion sections with areas
    - All data extracted from structured HTML elements

Bretton Woods Specific Features:
    - EXTENSIVE detailed metrics (acreage, mileage, counts)
    - Snowfall tracked by elevation (base vs upper mountain)
    - Trails organized by AREA (Mount Rosebrook, West Mountain, Zephyr)
    - Glades are SEPARATE from trails (not mixed)
    - T-bar lift (Telegraph T-Bar)
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

def extract_difficulty_from_svg(item):
    """Extract difficulty level from SVG icon class names"""
    svg = item.find('svg', class_=lambda c: c and 'trail-' in c if c else False)
    if not svg:
        return None
    
    classes = ' '.join(svg.get('class', []))
    if 'trail-circle' in classes:
        return 'green'
    elif 'trail-square' in classes:
        return 'blue'
    elif 'trail-diamond' in classes:
        return 'black'
    elif 'trail-two-diamond' in classes:
        return 'double-black'
    return None

def has_grooming_icon(item):
    """Check if item has grooming SVG icon"""
    return bool(item.find('svg', class_=lambda c: c and 'grooming' in c if c else False))

def scrape_bretton_woods():
    """
    Comprehensive scrape of Bretton Woods snow report (v2.1 FIXED)
    """
    url = 'https://www.brettonwoods.com/snow-trail-report/'
    
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
            'mountain': 'bretton-woods',
            'scraped_at': datetime.now().isoformat(),
            'source_url': url,
            'scraper_version': '2.1'
        },
        'summary_metrics': {},
        'weather': {},
        'lifts': [],
        'trails': [],
        'glades': [],
        'terrain_parks': [],
        'narrative_report': ''
    }

    # ========================================================================
    # SECTION 1: Extract Summary Metrics (keep existing working code)
    # ========================================================================
    print("\n[1/7] Extracting summary metrics...")
    try:
        page_text = soup.get_text()
        
        if 'Trail Count' in page_text:
            match = re.search(r'Trail Count\s*(\d+/\d+)', page_text)
            if match:
                data['summary_metrics']['open_trails'] = match.group(1)
        
        if 'Glades Count' in page_text:
            match = re.search(r'Glades Count\s*(\d+/\d+)', page_text)
            if match:
                data['summary_metrics']['open_glades'] = match.group(1)
        
        if 'Lifts Open' in page_text:
            match = re.search(r'Lifts Open\s*(\d+/\d+)', page_text)
            if match:
                data['summary_metrics']['open_lifts'] = match.group(1)
        
        if 'Total Acreage' in page_text:
            match = re.search(r'Total Acreage\s*([\d.]+)/([\d.]+)', page_text)
            if match:
                data['summary_metrics']['total_acreage'] = f"{match.group(1)}/{match.group(2)}"
        
        if 'Total Miles' in page_text:
            match = re.search(r'Total Miles\s*([\d.]+)/([\d.]+)', page_text)
            if match:
                data['summary_metrics']['total_miles'] = f"{match.group(1)}/{match.group(2)}"
        
        # Snowfall data
        snowfall_section = soup.find(string=re.compile(r'Season to Date'))
        if snowfall_section:
            parent = snowfall_section.find_parent()
            if parent:
                text = parent.get_text()
                recent_match = re.search(r'Recent\s*(\d+)"', text)
                season_match = re.search(r'Season to Date\s*(\d+)"', text)
                if recent_match:
                    data['summary_metrics']['snowfall_recent'] = recent_match.group(1) + '"'
                if season_match:
                    data['summary_metrics']['snowfall_season'] = season_match.group(1) + '"'
        
        print(f"   ✓ Extracted {len(data['summary_metrics'])} summary metrics")
        
    except Exception as e:
        print(f"   ✗ Warning: Could not extract summary metrics: {e}")

    # ========================================================================
    # SECTION 2: Extract Lifts (FIXED - using HTML structure)
    # ========================================================================
    print("\n[2/7] Extracting lifts...")
    try:
        # Find the accordion button with "Lifts Operating"
        lifts_button = soup.find('button', attrs={'aria-label': lambda x: x and 'Lifts Operating' in x})
        
        if lifts_button:
            # Navigate to accordion content
            accordion_content = lifts_button.find_next_sibling('div', class_='accordion__content-container')
            
            if accordion_content:
                # Find the table inside
                lifts_table = accordion_content.find('ul', class_='trail-reports__table')
                
                if lifts_table:
                    # Extract each lift
                    lift_items = lifts_table.find_all('li', class_='trail-reports__table-item')
                    
                    for item in lift_items:
                        # Get status from data-status attribute
                        status = item.get('data-status', 'unknown')
                        
                        # Get name from name div
                        name_div = item.find('div', class_='trail-reports__table-item--name')
                        if name_div:
                            name = name_div.get_text(strip=True)
                            
                            data['lifts'].append({
                                'name': name,
                                'status': status,
                                'hours': None
                            })
        
        print(f"   ✓ Extracted {len(data['lifts'])} lifts")
        
        # Check for T-Bar
        t_bar = [lift for lift in data['lifts'] if 't-bar' in lift['name'].lower()]
        if t_bar:
            print(f"   ℹ Found T-Bar: {t_bar[0]['name']}")
        
    except Exception as e:
        print(f"   ✗ Warning: Could not extract lifts: {e}")

    # ========================================================================
    # SECTION 3: Extract Trails by AREA (FIXED - using HTML structure)
    # ========================================================================
    print("\n[3/7] Extracting trails by area...")
    try:
        # Find the Alpine Trails section
        trails_container = soup.find('div', class_='trail-reports__container')
        
        if trails_container:
            # Find heading with "Alpine Trails"
            heading = trails_container.find('h4', string=lambda x: x and 'Alpine Trails' in x if x else False)
            
            if heading:
                # Find the accordion after the heading
                accordion = heading.find_next('ul', class_='accordion')
                
                if accordion:
                    # Find all accordion items (each is an area)
                    area_items = accordion.find_all('li', class_='accordion__item')
                    
                    for area_item in area_items:
                        # Get area name from button aria-label
                        area_button = area_item.find('button', class_='accordion__title')
                        if not area_button:
                            continue
                        
                        aria_label = area_button.get('aria-label', '')
                        # Parse "Mount Rosebrook 38/40" -> "Mount Rosebrook"
                        area_match = re.match(r'(.+?)\s+\d+/\d+', aria_label)
                        area_name = area_match.group(1) if area_match else aria_label
                        
                        # Find trails table in this area
                        trails_table = area_item.find('ul', class_='trail-reports__table')
                        if not trails_table:
                            continue
                        
                        # Extract each trail
                        trail_items = trails_table.find_all('li', class_='trail-reports__table-item')
                        
                        for item in trail_items:
                            # Get status
                            status = item.get('data-status', 'unknown')
                            
                            # Get name
                            name_div = item.find('div', class_='trail-reports__table-item--name')
                            if not name_div:
                                continue
                            
                            name = name_div.get_text(strip=True)
                            
                            # Get difficulty from SVG
                            difficulty = extract_difficulty_from_svg(item)
                            
                            # Check for grooming
                            groomed = has_grooming_icon(item)
                            
                            data['trails'].append({
                                'name': name,
                                'status': status,
                                'area': area_name,
                                'difficulty': difficulty,
                                'conditions': 'groomed' if groomed else None,
                                'night_skiing': False
                            })
                        
                        print(f"   ℹ {area_name}: {len(trail_items)} trails")
        
        print(f"   ✓ Extracted {len(data['trails'])} trails")
        
    except Exception as e:
        print(f"   ✗ Warning: Could not extract trails: {e}")

    # ========================================================================
    # SECTION 4: Extract Glades by AREA (FIXED - using HTML structure)
    # ========================================================================
    print("\n[4/7] Extracting glades by area...")
    try:
        # Find the Glades section (separate from trails)
        glades_containers = soup.find_all('div', class_='trail-reports__container')
        
        for container in glades_containers:
            heading = container.find('h4', string=lambda x: x and 'Glades' in x if x else False)
            
            if heading:
                # Find accordion for glades
                accordion = heading.find_next('ul', class_='accordion')
                
                if accordion:
                    # Find all glade areas
                    area_items = accordion.find_all('li', class_='accordion__item')
                    
                    for area_item in area_items:
                        # Get area name
                        area_button = area_item.find('button', class_='accordion__title')
                        if not area_button:
                            continue
                        
                        aria_label = area_button.get('aria-label', '')
                        # Parse "Mount Rosebrook Glades 4/19" -> "Mount Rosebrook"
                        area_match = re.match(r'(.+?)\s+(?:Glades\s+)?\d+/\d+', aria_label)
                        area_name = area_match.group(1) if area_match else aria_label
                        
                        # Find glades table
                        glades_table = area_item.find('ul', class_='trail-reports__table')
                        if not glades_table:
                            continue
                        
                        # Extract each glade
                        glade_items = glades_table.find_all('li', class_='trail-reports__table-item')
                        
                        for item in glade_items:
                            status = item.get('data-status', 'unknown')
                            
                            name_div = item.find('div', class_='trail-reports__table-item--name')
                            if not name_div:
                                continue
                            
                            name = name_div.get_text(strip=True)
                            difficulty = extract_difficulty_from_svg(item)
                            
                            data['glades'].append({
                                'name': name,
                                'status': status,
                                'area': area_name,
                                'difficulty': difficulty,
                                'conditions': None
                            })
                        
                        print(f"   ℹ {area_name}: {len(glade_items)} glades")
        
        print(f"   ✓ Extracted {len(data['glades'])} glades")
        
    except Exception as e:
        print(f"   ✗ Warning: Could not extract glades: {e}")

    # ========================================================================
    # SECTION 5: Extract Narrative Report
    # ========================================================================
    print("\n[5/7] Extracting narrative report...")
    try:
        # Look for comments section
        comments_section = soup.find(string=re.compile(r'Comments'))
        if comments_section:
            parent = comments_section.find_parent()
            if parent:
                # Get all text after "Comments" heading
                narrative_parts = []
                for sibling in parent.find_next_siblings():
                    text = sibling.get_text(strip=True)
                    if text and len(text) > 20:  # Skip short snippets
                        narrative_parts.append(text)
                    if len(narrative_parts) > 10:  # Reasonable limit
                        break
                
                data['narrative_report'] = '\n\n'.join(narrative_parts)
        
        print(f"   ✓ Extracted narrative ({len(data['narrative_report'])} chars)")
    except Exception as e:
        print(f"   ✗ Warning: Could not extract narrative: {e}")

    # ========================================================================
    # Final Summary
    # ========================================================================
    print("\n" + "="*60)
    print("SCRAPING COMPLETE - BRETTON WOODS v2.1")
    print("="*60)
    print(f"Summary Metrics: {len(data['summary_metrics'])} fields")
    print(f"Lifts: {len(data['lifts'])} items")
    print(f"Trails: {len(data['trails'])} items")
    areas = set(t['area'] for t in data['trails'] if t.get('area'))
    print(f"  - Areas: {', '.join(sorted(areas))}")
    print(f"Glades: {len(data['glades'])} items")
    glade_areas = set(g['area'] for g in data['glades'] if g.get('area'))
    print(f"  - Glade Areas: {', '.join(sorted(glade_areas))}")
    print(f"Narrative: {len(data['narrative_report'])} chars")
    print("="*60 + "\n")
    
    return data

def save_to_bronze(**context):
    """Save scraped data to Bronze layer in MinIO"""
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
            'scraper_version': data['metadata']['scraper_version']
        }
    )
    
    print(f"✓ Saved to Bronze layer: s3://{bucket_name}/{object_key}")
    print(f"✓ File size: {len(json_data)} bytes")
    
    return object_key

def ping_healthcheck(**context):
    """Ping Healthchecks.io after successful scrape"""
    try:
        ping_url = Variable.get('healthchecks_bretton_url')
        response = requests.get(ping_url, timeout=10)
        
        if response.status_code == 200:
            print("Successfully pinged Healthchecks.io")
    except Exception as e:
        print(f"Could not ping Healthchecks.io: {e}")

# Define the DAG
with DAG(
    dag_id='scrape_bretton_woods_v2',
    default_args=default_args,
    description='Daily scrape of Bretton Woods snow report (v2.1 - fixed HTML parsing)',
    schedule='0 12 * * *',
    start_date=datetime(2026, 1, 22),
    catchup=False,
    tags=['scraper', 'bretton-woods', 'bronze', 'v2'],
) as dag:
    
    # Task 1: Scrape the website
    scrape_task = PythonOperator(
        task_id='scrape_snow_report',
        python_callable=scrape_bretton_woods,
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