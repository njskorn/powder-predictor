"""
Silver Layer DAG - Transform Bronze to Silver
==============================================

This DAG reads Bronze layer data and transforms it into standardized Silver layer format
Runs after Bronze scrapers complete (triggered or on schedule)

Architecture:
    Bronze (s3://bronze-snow-reports/{mountain}/daily/{date}/{timestamp}.json)
      ↓
    Silver (s3://silver-snow-reports/{mountain}/daily/{date}/{timestamp}.json)
      ↓
    Current pointer (s3://silver-snow-reports/{mountain}/current.json)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import json
import boto3
from botocore.client import Config
import os

# Import transformation functions
from transform_bretton_woods import transform_bretton_woods
from transform_cannon import transform_cannon
from transform_cranmore import transform_cranmore

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

def get_latest_bronze(mountain: str) -> dict:
    """
    Fetch the most recent Bronze layer data for a mountain
    
    Args:
        mountain: Mountain name (bretton-woods, cannon, cranmore)
        
    Returns:
        Bronze layer JSON data
    """
    s3 = create_minio_client()
    bucket = 'bronze-snow-reports'
    
    # List objects for today
    date_str = datetime.now().strftime('%Y-%m-%d')
    prefix = f'{mountain}/daily/{date_str}/'
    
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        if 'Contents' not in response:
            raise ValueError(f"No Bronze data found for {mountain} on {date_str}")
        
        # Sort by timestamp (filename) and get latest
        objects = sorted(response['Contents'], key=lambda x: x['Key'], reverse=True)
        latest_key = objects[0]['Key']
        
        # Fetch the object
        obj = s3.get_object(Bucket=bucket, Key=latest_key)
        bronze_data = json.loads(obj['Body'].read().decode('utf-8'))
        
        print(f"✓ Loaded Bronze data from: s3://{bucket}/{latest_key}")
        return bronze_data
        
    except Exception as e:
        print(f"✗ Error fetching Bronze data for {mountain}: {e}")
        raise

def save_to_silver(mountain: str, silver_data: dict):
    """
    Save Silver layer data to MinIO
    
    Saves to both:
    1. Timestamped file: s3://silver-snow-reports/{mountain}/daily/{date}/{timestamp}.json
    2. Current pointer: s3://silver-snow-reports/{mountain}/current.json
    
    Args:
        mountain: Mountain name
        silver_data: Transformed Silver layer JSON
    """
    s3 = create_minio_client()
    bucket = 'silver-snow-reports'
    
    # Create bucket if it doesn't exist
    try:
        s3.head_bucket(Bucket=bucket)
    except:
        s3.create_bucket(Bucket=bucket)
        print(f"✓ Created bucket: {bucket}")
    
    date_str = datetime.now().strftime('%Y-%m-%d')
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save timestamped version
    daily_key = f'{mountain}/daily/{date_str}/{timestamp_str}.json'
    json_data = json.dumps(silver_data, indent=2)
    
    s3.put_object(
        Bucket=bucket,
        Key=daily_key,
        Body=json_data.encode('utf-8'),
        ContentType='application/json',
        Metadata={
            'mountain': mountain,
            'silver_version': silver_data.get('silver_version', '1.0'),
            'processed_date': date_str
        }
    )
    
    print(f"✓ Saved to: s3://{bucket}/{daily_key}")
    
    # Save as current.json (pointer to latest)
    current_key = f'{mountain}/current.json'
    s3.put_object(
        Bucket=bucket,
        Key=current_key,
        Body=json_data.encode('utf-8'),
        ContentType='application/json',
        Metadata={
            'mountain': mountain,
            'silver_version': silver_data.get('silver_version', '1.0'),
            'last_updated': timestamp_str
        }
    )
    
    print(f"✓ Updated: s3://{bucket}/{current_key}")
    print(f"✓ Data freshness: {silver_data['data_freshness']}")
    
    return daily_key

# Task functions for each mountain
def transform_bretton_task(**context):
    """Transform Bretton Woods Bronze → Silver"""
    print("\n" + "="*60)
    print("TRANSFORMING BRETTON WOODS: Bronze → Silver")
    print("="*60)
    
    bronze_data = get_latest_bronze('bretton-woods')
    silver_data = transform_bretton_woods(bronze_data)
    output_key = save_to_silver('bretton-woods', silver_data)
    
    # Summary
    print("\nTransformation Summary:")
    print(f"  Lifts: {silver_data['summary']['lifts']}")
    print(f"  Trails: {silver_data['summary']['trails']['open']}/{silver_data['summary']['trails']['total']}")
    print(f"  Glades: {silver_data['summary']['glades']['open']}/{silver_data['summary']['glades']['total']}")
    print(f"  Stale: {silver_data['data_freshness']['is_stale']}")
    print("="*60 + "\n")
    
    return output_key

def transform_cannon_task(**context):
    """Transform Cannon Bronze → Silver"""
    print("\n" + "="*60)
    print("TRANSFORMING CANNON: Bronze → Silver")
    print("="*60)
    
    bronze_data = get_latest_bronze('cannon')
    silver_data = transform_cannon(bronze_data)
    output_key = save_to_silver('cannon', silver_data)
    
    # Summary
    print("\nTransformation Summary:")
    print(f"  Lifts: {silver_data['summary']['lifts']}")
    print(f"  Trails: {silver_data['summary']['trails']['open']}/{silver_data['summary']['trails']['total']}")
    print(f"  Glades: {silver_data['summary']['glades']['open']}/{silver_data['summary']['glades']['total']}")
    print(f"  Temperature: {silver_data['weather']['temperature_base']}")
    print(f"  Stale: {silver_data['data_freshness']['is_stale']}")
    print("="*60 + "\n")
    
    return output_key

def transform_cranmore_task(**context):
    """Transform Cranmore Bronze → Silver"""
    print("\n" + "="*60)
    print("TRANSFORMING CRANMORE: Bronze → Silver")
    print("="*60)
    
    bronze_data = get_latest_bronze('cranmore')
    silver_data = transform_cranmore(bronze_data)
    output_key = save_to_silver('cranmore', silver_data)
    
    # Summary
    print("\nTransformation Summary:")
    print(f"  Lifts: {silver_data['summary']['lifts']}")
    print(f"  Trails: {silver_data['summary']['trails']['open']}/{silver_data['summary']['trails']['total']}")
    print(f"  Glades: {silver_data['summary']['glades']['open']}/{silver_data['summary']['glades']['total']}")
    print(f"  Snowfall: {silver_data['weather']['snowfall_recent']}")
    print(f"  Stale: {silver_data['data_freshness']['is_stale']}")
    print("="*60 + "\n")
    
    return output_key

# Define the DAG
with DAG(
    dag_id='transform_bronze_to_silver',
    default_args=default_args,
    description='Transform Bronze layer to Silver layer for all mountains',
    schedule='15 12 * * *',  # Run 15 min after Bronze scrapers (12:15 PM daily)
    start_date=datetime(2026, 1, 23),
    catchup=False,
    tags=['silver', 'transformation', 'data-quality'],
) as dag:
    
    # Sensor tasks - wait for Bronze scrapers to complete
    # (Optional: can also trigger this DAG from Bronze DAGs)
    
    # Transform tasks (can run in parallel)
    transform_bretton = PythonOperator(
        task_id='transform_bretton_woods',
        python_callable=transform_bretton_task,
    )
    
    transform_cannon = PythonOperator(
        task_id='transform_cannon',
        python_callable=transform_cannon_task,
    )
    
    transform_cranmore = PythonOperator(
        task_id='transform_cranmore',
        python_callable=transform_cranmore_task,
    )
    
    # All three transformations run in parallel
    # No dependencies between them