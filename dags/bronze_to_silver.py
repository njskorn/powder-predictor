"""
Silver Layer Transformation - Daily Conditions Summary
Purpose: Transform Bronze raw JSON into summmary wide table
Status: MVP - Basic OBT for quick analytics (will refactor later)
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
from botocore.client import Config
import json
import re
import os

default_args = {
    'owner': 'nettle',
    'retries': 2,
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

def fetch_latest_bronze_reports():
    """
    Fetch the most recent report for each mountain from Bronze layer
    Returns dict of {mountain_name: json_data}
    """
    s3 = create_minio_client()
    mountains = ['cranmore', 'bretton-woods', 'cannon']
    reports = {}
    
    for mountain in mountains:
        try:
            # List all objects for this mountain
            prefix = f'{mountain}/daily/'
            response = s3.list_objects_v2(Bucket='bronze-snow-reports', Prefix=prefix)
            
            if 'Contents' not in response:
                print(f"WARNING: No data found for {mountain}")
                continue
            
            # Sort by last modified, get most recent
            objects = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
            latest_key = objects[0]['Key']
            
            # Fetch the object
            obj = s3.get_object(Bucket='bronze-snow-reports', Key=latest_key)
            data = json.loads(obj['Body'].read().decode('utf-8'))
            
            reports[mountain] = data
            print(f"Loaded {mountain}: {latest_key}")
            
        except Exception as e:
            print(f"WARNING: Error fetching {mountain}: {e}")
    
    return reports

def extract_numeric(text):
    """Extract numeric value from strings like '79"' or '4 inches'"""
    if text is None or text == '':
        return None
    match = re.search(r'([\d.]+)', str(text))
    return float(match.group(1)) if match else None

def parse_trail_count(data):
    """Parse trail counts - handles '36/61' or '52' formats"""
    # Try different field names (different mountains use different keys)
    value = data.get('open_trails') or data.get('trails_open')
    
    if not value:
        return {'trails_open': None, 'trails_total': None, 'pct_trails_open': None}
    
    value_str = str(value)
    if '/' in value_str:
        # Format: "36/61"
        parts = value_str.split('/')
        open_count = int(parts[0])
        total_count = int(parts[1])
        pct = round((open_count / total_count) * 100, 1) if total_count > 0 else None
        return {
            'trails_open': open_count,
            'trails_total': total_count,
            'pct_trails_open': pct
        }
    else:
        # Format: "52" (just a number)
        return {
            'trails_open': int(value_str),
            'trails_total': None,
            'pct_trails_open': None
        }

def parse_lift_count(data):
    """Parse lift counts - handles '7/9' or '8' formats"""
    # Try different field names (different mountains use different keys)
    value = data.get('open_lifts') or data.get('lifts_open')
    
    if not value:
        return {'lifts_open': None, 'lifts_total': None, 'pct_lifts_open': None}
    
    value_str = str(value)
    if '/' in value_str:
        parts = value_str.split('/')
        open_count = int(parts[0])
        total_count = int(parts[1])
        pct = round((open_count / total_count) * 100, 1) if total_count > 0 else None
        return {
            'lifts_open': open_count,
            'lifts_total': total_count,
            'pct_lifts_open': pct
        }
    else:
        return {
            'lifts_open': int(value_str),
            'lifts_total': None,
            'pct_lifts_open': None
        }

def parse_snowfall(data, mountain_name):
    """
    Parse snowfall data - handles different formats per mountain
    Returns: {season_snowfall_inches, recent_snowfall_inches, recent_snowfall_period}
    """
    result = {
        'season_snowfall_inches': None,
        'recent_snowfall_inches': None,
        'recent_snowfall_period': None
    }
    
    # Season snowfall (all mountains have this, different field names)
    if 'season_snowfall' in data:  # Cannon
        result['season_snowfall_inches'] = extract_numeric(data['season_snowfall'])
    elif 'season_total' in data:  # Cranmore
        result['season_snowfall_inches'] = extract_numeric(data['season_total'])
    elif 'season_snowfall_base' in data:  # Bretton Woods (use base mountain)
        result['season_snowfall_inches'] = extract_numeric(data['season_snowfall_base'])
    
    # Recent snowfall (mountain-specific)
    if mountain_name == 'cranmore':
        # Cranmore: 7-day total
        if 'seven_day_snowfall' in data:
            result['recent_snowfall_inches'] = extract_numeric(data['seven_day_snowfall'])
            result['recent_snowfall_period'] = '7 days'
    
    elif mountain_name == 'bretton-woods':
        # Bretton Woods: "recent" (period not specified)
        if 'recent_snowfall_base' in data:
            result['recent_snowfall_inches'] = extract_numeric(data['recent_snowfall_base'])
            result['recent_snowfall_period'] = None
    
    elif mountain_name == 'cannon':
        # Cannon: In narrative text (TODO: LLM extraction later)
        # For now, leave as NULL
        pass
    
    return result

def parse_mountain_data(mountain_name, data):
    """
    Parse a single mountain's Bronze data into Silver row
    """
    row = {
        'mountain_name': mountain_name,
        'report_date': datetime.fromisoformat(data['scraped_at']).date().isoformat(),
        'scraped_timestamp': data['scraped_at'],
    }
    
    # Parse trail counts (pass whole data object)
    trail_data = parse_trail_count(data)
    row.update(trail_data)
    
    # Parse lift counts (pass whole data object)
    lift_data = parse_lift_count(data)
    row.update(lift_data)
    
    # Optional metrics - handle fraction formats
    row['glades_open'] = data.get('glades_open')
    
    # Parse acres and miles (handle "365.8/470.05" format for Bretton Woods)
    acres_value = data.get('acres') or data.get('total_acreage')
    if acres_value:
        if '/' in str(acres_value):
            # Format: "365.8/470.05" - take the open value (first part)
            row['acres'] = extract_numeric(str(acres_value).split('/')[0])
        else:
            row['acres'] = extract_numeric(acres_value)
    else:
        row['acres'] = None
    
    miles_value = data.get('miles') or data.get('total_miles')
    if miles_value:
        if '/' in str(miles_value):
            # Format: "28.984/36.884" - take the open value (first part)
            row['miles'] = extract_numeric(str(miles_value).split('/')[0])
        else:
            row['miles'] = extract_numeric(miles_value)
    else:
        row['miles'] = None
    
    # Parse snowfall
    snowfall_data = parse_snowfall(data, mountain_name)
    row.update(snowfall_data)
    
    # Conditions
    row['base_depth'] = data.get('base_depth')
    row['primary_surface'] = data.get('primary_surface')
    row['secondary_surface'] = data.get('secondary_surface')
    row['snow_conditions'] = data.get('snow_conditions')
    row['grooming'] = data.get('grooming')
    row['snowmaking'] = data.get('snowmaking')
    
    # Reference
    row['last_updated'] = data.get('last_updated')
    
    return row

def transform_to_silver(**context):
    """
    Transform Bronze JSON reports into Silver wide table
    Saves as JSON (will switch to Parquet later with proper deps)
    """
    # Fetch latest Bronze data
    reports = fetch_latest_bronze_reports()
    
    if not reports:
        raise ValueError("No Bronze data available to transform")
    
    print(f"Fetched {len(reports)} mountain reports")
    
    # Transform each mountain's data
    rows = []
    for mountain_name, data in reports.items():
        try:
            row = parse_mountain_data(mountain_name, data)
            rows.append(row)
            print(f"Parsed {mountain_name}")
        except Exception as e:
            print(f"WARNING: Error parsing {mountain_name}: {e}")
            raise
    
    # Print summary
    print("\nSilver Layer Summary:")
    for row in rows:
        trails_display = f"{row['trails_open'] or 'N/A'}/{row['trails_total'] or 'N/A'}"
        pct_display = f"({row['pct_trails_open'] or 'N/A'}%)"
        recent_display = f"{row['recent_snowfall_inches'] or 'N/A'}\" ({row['recent_snowfall_period'] or 'N/A'})"
        season_display = f"{row['season_snowfall_inches'] or 'N/A'}\""
        
        print(f"  {row['mountain_name']:15} | Trails: {trails_display:10} {pct_display:8} | "
              f"Recent: {recent_display:20} | Season: {season_display}")
    
    # Save to JSON (temporary format, will upgrade to Parquet later)
    output_path = '/tmp/daily_conditions.json'
    with open(output_path, 'w') as f:
        json.dump(rows, f, indent=2)
    print(f"Saved to: {output_path}")
    
    # Upload to MinIO Silver layer
    s3 = create_minio_client()
    
    # Create bucket if it doesn't exist
    try:
        s3.head_bucket(Bucket='silver-snow-reports')
    except:
        s3.create_bucket(Bucket='silver-snow-reports')
        print("Created silver-snow-reports bucket")
    
    # Upload JSON file
    date_str = datetime.now().strftime('%Y-%m-%d')
    s3_key = f'daily/{date_str}/conditions.json'
    
    with open(output_path, 'rb') as f:
        s3.put_object(
            Bucket='silver-snow-reports',
            Key=s3_key,
            Body=f,
            ContentType='application/json',
            Metadata={'date': date_str}
        )
    
    print(f"Uploaded to Silver: s3://silver-snow-reports/{s3_key}")
    
    return s3_key

# Define the DAG
with DAG(
    dag_id='bronze_to_silver',
    default_args=default_args,
    description='Transform Bronze reports to Silver analytics table (MVP wide table)',
    schedule='15 12 * * *',  # Run at 1215 PM UTC (0715 AM EST), after scrapers finish
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=['transform', 'silver', 'analytics'],
) as dag:
    
    transform = PythonOperator(
        task_id='transform_to_silver',
        python_callable=transform_to_silver,
    )