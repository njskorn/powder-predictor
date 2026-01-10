"""
MinIO Bucket Setup DAG
Purpose: Create bronze/silver/gold buckets and test connectivity
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
from botocore.client import Config
import json

default_args = {
    'owner': 'nettle',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

def create_minio_client():
    """Create and return configured MinIO client"""
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',  # MinIO service name from docker-compose
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minio_secure_2025',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'  # MinIO doesn't care, but boto3 wants it
    )
    return s3_client

def create_buckets():
    """Create initial bronze, silver, and gold buckets"""
    s3 = create_minio_client()
    
    buckets = ['bronze-snow-reports', 'silver-snow-reports', 'gold-snow-reports']
    
    for bucket_name in buckets:
        try:
            # Check if bucket exists
            s3.head_bucket(Bucket=bucket_name)
            print(f" Bucket '{bucket_name}' already exists")
        except:
            # Create bucket if it doesn't exist
            s3.create_bucket(Bucket=bucket_name)
            print(f" Created bucket '{bucket_name}'")
    
    print("\n All medallion architecture buckets are ready!")
    return "Buckets created successfully"

def write_test_file():
    """Write a test file to bronze bucket"""
    s3 = create_minio_client()
    
    # Create sample test data
    test_data = {
        'message': 'Hello from Airflow!',
        'mountain': 'test-mountain',
        'snowfall': '12 inches',
        'timestamp': datetime.now().isoformat()
    }
    
    # Convert to JSON string
    json_data = json.dumps(test_data, indent=2)
    
    # Write to bronze bucket
    bucket_name = 'bronze-snow-reports'
    object_key = 'test/hello_minio.json'
    s3.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=json_data.encode('utf-8'),
        ContentType='application/json'
    )
    
    print(f" Wrote test file to: {bucket_name}/{object_key}")
    return f"File written to {bucket_name}/{object_key}"

def read_test_file():
    """Read the test file back from bronze bucket"""
    s3 = create_minio_client()
    
    # hardcode test file
    bucket_name = 'bronze-snow-reports'
    object_key = 'test/hello_minio.json'
    
    # Read the file
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    content = response['Body'].read().decode('utf-8')
    
    # Parse JSON
    data = json.loads(content)
    
    print(f" Successfully read file from MinIO!")
    print(f" Content: {json.dumps(data, indent=2)}")
    
    return "File read successfully"

def list_buckets():
    """List all buckets to verify everything worked"""
    s3 = create_minio_client()
    
    response = s3.list_buckets()
    
    print("\n Buckets in MinIO:")
    for bucket in response['Buckets']:
        print(f"  - {bucket['Name']}")
    
    return f"Found {len(response['Buckets'])} buckets"

with DAG(
    dag_id='setup_minio_buckets',
    default_args=default_args,
    description='Set up medallion architecture buckets in MinIO',
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 1, 10),
    catchup=False,
    tags=['setup', 'minio', 'infrastructure'],
) as dag:
    
    # Task 1: Create buckets
    create_buckets_task = PythonOperator(
        task_id='create_medallion_buckets',
        python_callable=create_buckets,
    )
    
    # Task 2: Write test file
    write_test_task = PythonOperator(
        task_id='write_test_file',
        python_callable=write_test_file,
    )
    
    # Task 3: Read test file
    read_test_task = PythonOperator(
        task_id='read_test_file',
        python_callable=read_test_file,
    )
    
    # Task 4: List all buckets
    list_buckets_task = PythonOperator(
        task_id='list_all_buckets',
        python_callable=list_buckets,
    )
    
    # Define dependencies
    create_buckets_task >> write_test_task >> read_test_task >> list_buckets_task