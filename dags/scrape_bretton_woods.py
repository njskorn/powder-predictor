"""
Bretton Woods Snow Report Scraper - VERSION 2.0 (ACTUAL STRUCTURE)
===================================================================

Built from actual HTML structure analysis on 2026-01-22

Bretton Woods Specific Features:
    - EXTENSIVE detailed metrics (acreage, mileage, counts)
    - Snowfall tracked by elevation (base vs upper mountain)
    - Trails organized by AREA (Mount Rosebrook, West Mountain, Zephyr)
    - Glades are SEPARATE from trails (not mixed)
    - T-bar lift (Telegraph T-Bar)
    - Weather notices/warnings from NWS
    - Very detailed narrative reports
    
NEW in v2.0:
    - Weather data extraction
    - Trail area organization (NEW dimension!)
    - Acreage and mileage metrics
    - Elevation-based snowfall tracking
    - No lift capacity in Bronze (moved to Silver layer)

Purpose: 
    Daily scraping of ALL available snow conditions from Bretton Woods
    - Detailed metrics (lifts, trails, glades, acreage, mileage, snowfall by elevation)
    - Individual lift details (name, status, hours)
    - Individual trail details WITH AREA (Mount Rosebrook, West Mountain, Zephyr)
    - Individual glade details WITH AREA (separate from trails)
    - Terrain parks
    - Full narrative snow report
    - Weather warnings

Architecture:
    Bronze layer = exactly what the website shows
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

def parse_count_format(text):
    """
    Parse format like "61/63" or "25/40"
    Returns (open, total) or (None, None) if can't parse
    """
    match = re.search(r'(\d+)/(\d+)', text)
    if match:
        return match.group(1), match.group(2)
    return None, None

def scrape_bretton_woods():
    """
    Comprehensive scrape of Bretton Woods snow report (v2.0)
    
    Returns nested JSON with:
        - metadata (mountain, timestamp, url, scraper version)
        - summary_metrics (extensive: lifts, trails, glades, acreage, mileage, snowfall by elevation)
        - weather (temperature, wind, conditions, NWS warnings)
        - lifts (array of lift objects)
        - trails (array with AREA field for geographic organization)
        - glades (array with AREA field - SEPARATE from trails!)
        - terrain_parks (array)
        - narrative_report (very detailed at Bretton Woods)
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
            'scraper_version': '2.0'
        },
        'summary_metrics': {},
        'weather': {},
        'lifts': [],
        'trails': [],  # Does NOT include glades at Bretton Woods
        'glades': [],  # Separate from trails!
        'terrain_parks': [],
        'narrative_report': ''
    }

    # ========================================================================
    # SECTION 1: Extract Summary Metrics
    # ========================================================================
    # Bretton Woods uses text-based format, not datapoint divs
    # Look for patterns like "Trail Count", "Glades Count", "Total Acreage"
    
    print("\n[1/7] Extracting summary metrics...")
    try:
        # Get the full page text to search for metrics
        page_text = soup.get_text()
        
        # Trail Count: 61/63
        if 'Trail Count' in page_text:
            match = re.search(r'Trail Count\s*(\d+/\d+)', page_text)
            if match:
                data['summary_metrics']['open_trails'] = match.group(1)
        
        # Glades Count: 25/40 (separate from trails!)
        if 'Glades Count' in page_text:
            match = re.search(r'Glades Count\s*(\d+/\d+)', page_text)
            if match:
                data['summary_metrics']['open_glades'] = match.group(1)
        
        # Lifts Open: 7/9
        if 'Lifts Open' in page_text:
            match = re.search(r'Lifts Open\s*(\d+/\d+)', page_text)
            if match:
                data['summary_metrics']['open_lifts'] = match.group(1)
        
        # Total Acreage: 447.05/470.05
        if 'Total Acreage' in page_text:
            match = re.search(r'Total Acreage\s*([\d.]+)/([\d.]+)', page_text)
            if match:
                data['summary_metrics']['total_acreage'] = f"{match.group(1)}/{match.group(2)}"
        
        # Total Miles: 33.684/36.884
        if 'Total Miles' in page_text:
            match = re.search(r'Total Miles\s*([\d.]+)/([\d.]+)', page_text)
            if match:
                data['summary_metrics']['total_miles'] = f"{match.group(1)}/{match.group(2)}"
        
        # Trail Acreage: 339.4 acres (98% - 27.014 miles)
        if 'Trail Acreage:' in page_text:
            match = re.search(r'Trail Acreage:\s*([\d.]+)\s*acres\s*\((\d+)%\s*-\s*([\d.]+)\s*miles\)', page_text)
            if match:
                data['summary_metrics']['trail_acreage'] = match.group(1)
                data['summary_metrics']['trail_acreage_pct'] = match.group(2)
                data['summary_metrics']['trail_miles'] = match.group(3)
        
        # Glade Acreage: 107.65 acres (84% - 6.67 miles)
        if 'Glade Acreage:' in page_text:
            match = re.search(r'Glade Acreage:\s*([\d.]+)\s*acres\s*\((\d+)%\s*-\s*([\d.]+)\s*miles\)', page_text)
            if match:
                data['summary_metrics']['glade_acreage'] = match.group(1)
                data['summary_metrics']['glade_acreage_pct'] = match.group(2)
                data['summary_metrics']['glade_miles'] = match.group(3)
        
        # Lift Hours
        if 'Lift Hours:' in page_text:
            match = re.search(r'Lift Hours:\s*([^]+)', page_text)
            if match:
                data['summary_metrics']['lift_hours'] = match.group(1).strip()
        
        # Grooming
        if 'Grooming:' in page_text:
            match = re.search(r'Grooming:\s*(\w+)', page_text)
            if match:
                data['summary_metrics']['grooming'] = match.group(1)
        
        # Snowmaking
        if 'Snowmaking:' in page_text:
            match = re.search(r'Snowmaking:\s*(\w+)', page_text)
            if match:
                data['summary_metrics']['snowmaking'] = match.group(1)
        
        # Snow Conditions
        if 'Snow Conditions:' in page_text:
            match = re.search(r'Snow Conditions:\s*([^\n]+)', page_text)
            if match:
                data['summary_metrics']['snow_conditions'] = match.group(1).strip()
        
        # Base Depth
        if 'Base Depth:' in page_text:
            match = re.search(r'Base Depth:\s*([^\n]+)', page_text)
            if match:
                data['summary_metrics']['base_depth'] = match.group(1).strip()
        
        print(f"   ✓ Extracted {len(data['summary_metrics'])} summary metrics")
        
    except Exception as e:
        print(f"   ✗ Warning: Could not extract summary metrics: {e}")
        data['summary_metrics']['extraction_error'] = str(e)

    # ========================================================================
    # SECTION 2: Extract Snowfall by Elevation
    # ========================================================================
    # Bretton Woods reports snowfall separately for base and upper mountain
    # Recent: 0" / 0"
    # Season to Date: 65" / 88"
    
    print("\n[2/7] Extracting snowfall by elevation...")
    try:
        # Look for Snowfall table with Mountain Base and Upper Mountain columns
        # Recent: 0" / 0"
        # Season to Date: 65" / 88"
        
        # Strategy: Find "Snowfall" header, then extract following data
        snowfall_section = soup.find(string=lambda t: t and 'Snowfall' in t and 'Mountain Base' in soup.get_text()[soup.get_text().find(t):soup.get_text().find(t)+200])
        
        if snowfall_section or 'Mountain Base' in page_text:
            # Recent snowfall
            recent_match = re.search(r'Recent\s*(\d+)"\s*(\d+)"', page_text)
            if recent_match:
                data['summary_metrics']['snowfall_recent_base'] = f'{recent_match.group(1)}"'
                data['summary_metrics']['snowfall_recent_upper'] = f'{recent_match.group(2)}"'
            
            # Season to date
            season_match = re.search(r'Season to Date\s*(\d+)"\s*(\d+)"', page_text)
            if season_match:
                data['summary_metrics']['snowfall_season_base'] = f'{season_match.group(1)}"'
                data['summary_metrics']['snowfall_season_upper'] = f'{season_match.group(2)}"'
        
        print(f"   ✓ Extracted snowfall by elevation (base vs upper)")
        
    except Exception as e:
        print(f"   ✗ Warning: Could not extract snowfall: {e}")

    # ========================================================================
    # SECTION 3: Extract Weather Data and Warnings
    # ========================================================================
    
    print("\n[3/7] Extracting weather and warnings...")
    try:
        # Current weather at top
        # Temperature: 24°F
        temp_match = re.search(r'(\d+)°F', page_text[:1000])  # Look in first 1000 chars
        if temp_match:
            data['weather']['temperature'] = f"{temp_match.group(1)}°F"
        
        # High/Low temps
        high_match = re.search(r'High\s*(\d+)°F', page_text[:1000])
        if high_match:
            data['weather']['temperature_high'] = f"{high_match.group(1)}°F"
        
        low_match = re.search(r'Low\s*(\d+)°F', page_text[:1000])
        if low_match:
            data['weather']['temperature_low'] = f"{low_match.group(1)}°F"
        
        # Weather conditions (e.g., "Chance Light Snow")
        conditions_elem = soup.find(string=re.compile(r'Chance|Light|Heavy|Snow|Rain|Clear|Cloudy'))
        if conditions_elem:
            # Get a few words of context
            conditions_text = conditions_elem.strip()
            if len(conditions_text) < 50:  # Reasonable length for weather condition
                data['weather']['conditions'] = conditions_text
        
        # Weather Notice/Warning (NWS alerts)
        weather_notice = soup.find(string=lambda t: t and 'Weather Notice:' in t)
        if weather_notice:
            # Get the next few siblings to capture the full warning
            parent = weather_notice.find_parent()
            if parent:
                warning_text = parent.get_text(strip=True)
                # Trim to reasonable length
                if len(warning_text) < 2000:
                    data['weather']['nws_warning'] = warning_text
        
        print(f"   ✓ Extracted {len(data['weather'])} weather fields")
        
    except Exception as e:
        print(f"   ✗ Warning: Could not extract weather: {e}")

    # ========================================================================
    # SECTION 4: Extract Narrative Report
    # ========================================================================
    
    print("\n[4/7] Extracting narrative report...")
    try:
        # Look for "Comments" section which contains the narrative
        comments_section = soup.find(string=lambda t: t and 'Forecasted Snow Report' in t)
        
        if comments_section:
            # Get parent element and extract text
            parent = comments_section.find_parent()
            if parent:
                # Get all paragraphs or text blocks
                narrative_parts = []
                for elem in parent.find_all(['p', 'div']):
                    text = elem.get_text(strip=True)
                    if text and len(text) > 50:  # Substantial text
                        narrative_parts.append(text)
                
                data['narrative_report'] = '\n\n'.join(narrative_parts[:10])  # Limit to first 10 paragraphs
        
        # Also try finding by "Comments" label
        if not data['narrative_report']:
            comments_label = soup.find(string=re.compile(r'Comments'))
            if comments_label:
                parent = comments_label.find_parent()
                if parent:
                    data['narrative_report'] = parent.get_text(strip=True)
        
        # Get "Updated" timestamp
        updated_match = re.search(r'Updated:\s*([^\n]+)', page_text)
        if updated_match:
            data['metadata']['last_updated'] = updated_match.group(1).strip()
        
        print(f"   ✓ Extracted narrative ({len(data['narrative_report'])} chars)")
        
    except Exception as e:
        print(f"   ✗ Warning: Could not extract narrative: {e}")
        data['narrative_report'] = ''

    # ========================================================================
    # SECTION 5: Extract Individual Lifts
    # ========================================================================
    
    print("\n[5/7] Extracting lift details...")
    try:
        # Find "Lifts Operating" section
        lifts_section = soup.find(string=re.compile(r'Lifts Operating'))
        
        if lifts_section:
            # Get parent container
            parent = lifts_section.find_parent()
            # Find list items
            lift_items = parent.find_next_siblings(['li', 'div']) if parent else []
            
            # Also try finding ul/li structure
            if not lift_items:
                ul = soup.find('ul', class_=lambda c: c and 'lift' in c.lower() if c else False) or \
                     soup.find_all('li', string=re.compile(r'Express|Quad|Gondola|T-Bar|Carpet'))
                if ul:
                    lift_items = ul if isinstance(ul, list) else ul.find_all('li')
            
            for item in lift_items:
                text = item.get_text(strip=True)
                
                # Parse format: "Bretton Woods Skyway Gondola - open"
                # or just look for lift names
                if any(keyword in text for keyword in ['Express', 'Quad', 'Gondola', 'T-Bar', 'Carpet', 'Triple', 'Double']):
                    # Split on status keywords
                    if ' open' in text.lower():
                        name = text.lower().replace(' open', '').strip()
                        name = name.replace('- ', '').strip()
                        status = 'open'
                    elif ' closed' in text.lower():
                        name = text.lower().replace(' closed', '').strip()
                        name = name.replace('- ', '').strip()
                        status = 'closed'
                    else:
                        name = text.strip()
                        status = 'unknown'
                    
                    # Capitalize name properly
                    name = ' '.join(word.capitalize() for word in name.split())
                    
                    if name:
                        data['lifts'].append({
                            'name': name,
                            'status': status,
                            'hours': None  # Not always specified per lift
                        })
        
        print(f"   ✓ Extracted {len(data['lifts'])} lifts")
        
        # Check for T-Bar
        t_bar = [lift for lift in data['lifts'] if 't-bar' in lift['name'].lower()]
        if t_bar:
            print(f"   ℹ Found T-Bar: {t_bar[0]['name']}")
        
    except Exception as e:
        print(f"   ✗ Warning: Could not extract lifts: {e}")

    # ========================================================================
    # SECTION 6: Extract Trails by AREA
    # ========================================================================
    # CRITICAL: Trails are organized by geographic area
    # Mount Rosebrook, West Mountain, Zephyr Area
    
    print("\n[6/7] Extracting trails by area...")
    try:
        # Find "Alpine Trails" section
        trails_header = soup.find(string=re.compile(r'Alpine Trails'))
        
        if trails_header:
            # Look for area headers like "Mount Rosebrook 38/40"
            area_pattern = re.compile(r'(Mount Rosebrook|West Mountain|Zephyr Area)\s*(\d+/\d+)')
            
            # Find all area sections
            current_area = None
            for elem in soup.find_all(['h3', 'li', 'div']):
                text = elem.get_text(strip=True)
                
                # Check if this is an area header
                area_match = area_pattern.search(text)
                if area_match:
                    current_area = area_match.group(1)
                    print(f"   ℹ Found area: {current_area} ({area_match.group(2)})")
                    continue
                
                # Check if this is a trail (has status keywords and is under an area)
                if current_area and any(keyword in text.lower() for keyword in [' open', ' closed']):
                    # Parse trail
                    if ' open' in text.lower():
                        name_parts = text.lower().split(' open')
                        name = name_parts[0].strip()
                        status = 'open'
                    elif ' closed' in text.lower():
                        name_parts = text.lower().split(' closed')
                        name = name_parts[0].strip()
                        status = 'closed'
                    else:
                        continue
                    
                    # Capitalize name
                    name = ' '.join(word.capitalize() if word else '' for word in name.split())
                    name = name.replace("'S", "'s")  # Fix possessives
                    
                    if name and len(name) > 2:  # Valid trail name
                        data['trails'].append({
                            'name': name,
                            'status': status,
                            'area': current_area,  # NEW FIELD!
                            'difficulty': None,  # Not always in text format
                            'conditions': None,
                            'night_skiing': False
                        })
        
        print(f"   ✓ Extracted {len(data['trails'])} trails")
        
    except Exception as e:
        print(f"   ✗ Warning: Could not extract trails: {e}")

    # ========================================================================
    # SECTION 7: Extract Glades by AREA (SEPARATE from trails!)
    # ========================================================================
    
    print("\n[7/7] Extracting glades by area...")
    try:
        # Find "Glades" section (separate from Alpine Trails)
        glades_header = soup.find(string=re.compile(r'Glades'))
        
        if glades_header:
            # Look for area headers like "Mount Rosebrook Glades 4/19"
            area_pattern = re.compile(r'(Mount Rosebrook|West Mountain|Zephyr|Stickney System)\s*(?:Glades)?\s*(\d+/\d+)')
            
            current_area = None
            for elem in soup.find_all(['h3', 'li', 'div']):
                text = elem.get_text(strip=True)
                
                # Check if this is a glade area header
                area_match = area_pattern.search(text)
                if area_match:
                    current_area = area_match.group(1)
                    print(f"   ℹ Found glade area: {current_area} ({area_match.group(2)})")
                    continue
                
                # Check if this is a glade (has status and is under a glade area)
                if current_area and any(keyword in text.lower() for keyword in [' open', ' closed']):
                    if ' open' in text.lower():
                        name = text.lower().replace(' open', '').strip()
                        status = 'open'
                    elif ' closed' in text.lower():
                        name = text.lower().replace(' closed', '').strip()
                        status = 'closed'
                    else:
                        continue
                    
                    # Capitalize
                    name = ' '.join(word.capitalize() for word in name.split())
                    name = name.replace("'S", "'s")
                    
                    if name and len(name) > 2:
                        data['glades'].append({
                            'name': name,
                            'status': status,
                            'area': current_area,  # NEW FIELD!
                            'difficulty': None,
                            'conditions': None
                        })
        
        print(f"   ✓ Extracted {len(data['glades'])} glades")
        
    except Exception as e:
        print(f"   ✗ Warning: Could not extract glades: {e}")

    # ========================================================================
    # Note: Terrain parks would be extracted if present
    # Bretton Woods may not have terrain parks or they're minimal
    # ========================================================================

    # ========================================================================
    # Final Summary
    # ========================================================================
    
    print("\n" + "="*60)
    print("SCRAPING COMPLETE - BRETTON WOODS v2.0")
    print("="*60)
    print(f"Summary Metrics: {len(data['summary_metrics'])} fields")
    print(f"Weather Data: {len(data['weather'])} fields")
    print(f"Lifts: {len(data['lifts'])} items")
    print(f"Trails: {len(data['trails'])} items")
    areas = set(t['area'] for t in data['trails'] if t.get('area'))
    print(f"  - Areas: {', '.join(areas)}")
    print(f"Glades: {len(data['glades'])} items (SEPARATE from trails)")
    glade_areas = set(g['area'] for g in data['glades'] if g.get('area'))
    print(f"  - Glade Areas: {', '.join(glade_areas)}")
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

# Define the DAG
with DAG(
    dag_id='scrape_bretton_woods_v2',
    default_args=default_args,
    description='Daily scrape of Bretton Woods snow report (v2.0 - actual structure)',
    schedule='0 12 * * *',  # Run daily at 12 PM UTC (7 AM EST)
    start_date=datetime(2026, 1, 22),
    catchup=False,
    tags=['scraper', 'bretton-woods', 'bronze', 'v2'],
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