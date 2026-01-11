#!/usr/bin/env python3
"""
Compare Snow Conditions Across Mountains
Reads latest JSON files from MinIO Bronze layer and displays side-by-side comparison
"""
import boto3
from botocore.client import Config
import json
from datetime import datetime
import os

# MinIO configuration
MINIO_ENDPOINT = 'http://localhost:9000'
MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD', 'minio_secure_2025')
BUCKET_NAME = 'bronze-snow-reports'

def create_minio_client():
    """Create MinIO S3 client"""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

def get_latest_report(s3, mountain):
    """Get the most recent report for a mountain"""
    try:
        # List all objects for this mountain
        prefix = f'{mountain}/daily/'
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        
        if 'Contents' not in response:
            return None
        
        # Sort by last modified, get most recent
        objects = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
        latest_key = objects[0]['Key']
        
        # Fetch the object
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=latest_key)
        data = json.loads(obj['Body'].read().decode('utf-8'))
        
        return data
    except Exception as e:
        print(f"Error fetching {mountain}: {e}")
        return None

def format_report(mountain_name, data):
    """Format a single mountain's report"""
    if not data:
        return f"\n{mountain_name.upper()}: No data available\n"
    
    lines = []
    lines.append(f"\n{'='*50}")
    lines.append(f"{mountain_name.upper()}")
    lines.append(f"{'='*50}")
    
    # Last updated
    if 'last_updated' in data:
        lines.append(f"Last Updated: {data['last_updated']}")
    elif 'scraped_at' in data:
        scraped = datetime.fromisoformat(data['scraped_at'])
        lines.append(f"Scraped: {scraped.strftime('%Y-%m-%d %I:%M %p')}")
    
    lines.append("")
    
    # Core metrics
    lines.append("TRAIL & LIFT STATUS:")
    if 'open_trails' in data:
        lines.append(f"  Trails Open:     {data['open_trails']}")
    if 'open_lifts' in data:
        lines.append(f"  Lifts Open:      {data['open_lifts']}")
    if 'glades_open' in data:
        lines.append(f"  Glades Open:     {data['glades_open']}")
    
    lines.append("")
    
    # Snowfall
    lines.append("SNOWFALL:")
    if 'seven_day_snowfall' in data:
        lines.append(f"  7-Day Total:     {data['seven_day_snowfall']}")
    if 'recent_snowfall_base' in data and 'recent_snowfall_upper' in data:
        lines.append(f"  Recent (Base):   {data['recent_snowfall_base']}")
        lines.append(f"  Recent (Upper):  {data['recent_snowfall_upper']}")
    if 'season_total' in data:
        lines.append(f"  Season Total:    {data['season_total']}")
    if 'season_snowfall_base' in data and 'season_snowfall_upper' in data:
        lines.append(f"  Season (Base):   {data['season_snowfall_base']}")
        lines.append(f"  Season (Upper):  {data['season_snowfall_upper']}")
    
    lines.append("")
    
    # Conditions
    lines.append("CONDITIONS:")
    if 'base_depth' in data:
        lines.append(f"  Base Depth:      {data['base_depth']}")
    if 'snow_conditions' in data:
        lines.append(f"  Snow Type:       {data['snow_conditions']}")
    if 'grooming' in data:
        lines.append(f"  Grooming:        {data['grooming']}")
    if 'snowmaking' in data:
        lines.append(f"  Snowmaking:      {data['snowmaking']}")
    
    # Optional metrics
    if 'total_acreage' in data:
        lines.append(f"  Total Acreage:   {data['total_acreage']}")
    if 'total_miles' in data:
        lines.append(f"  Total Miles:     {data['total_miles']}")
    
    return "\n".join(lines)

def main():
    """Main comparison function"""
    print("\n" + "="*50)
    print("POWDER PREDICTOR - MOUNTAIN COMPARISON")
    print("="*50)
    
    s3 = create_minio_client()
    
    # Mountains to compare
    mountains = {
        'cranmore': 'Cranmore',
        'bretton-woods': 'Bretton Woods',
        # 'cannon': 'Cannon Mountain',  # Uncomment when scraper is ready
    }
    
    reports = {}
    for key, name in mountains.items():
        print(f"\nFetching {name}...", end=" ")
        data = get_latest_report(s3, key)
        if data:
            reports[name] = data
            print("✓")
        else:
            print("✗ (no data)")
    
    # Display all reports
    for name, data in reports.items():
        print(format_report(name, data))
    
    # Quick comparison summary
    if len(reports) > 1:
        print("\n" + "="*50)
        print("QUICK COMPARISON")
        print("="*50)
        
        for name, data in reports.items():
            trails = data.get('open_trails', 'N/A')
            lifts = data.get('open_lifts', 'N/A')
            snow = data.get('seven_day_snowfall') or data.get('recent_snowfall_base', 'N/A')
            print(f"{name:20} | Trails: {trails:8} | Lifts: {lifts:8} | Recent Snow: {snow}")
    
    print("\n" + "="*50)
    print(f"Report generated: {datetime.now().strftime('%Y-%m-%d %I:%M %p')}")
    print("="*50 + "\n")

if __name__ == '__main__':
    main()