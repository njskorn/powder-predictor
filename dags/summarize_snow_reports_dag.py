"""
AI Summarization DAG
====================

Runs AFTER transform DAGs to add AI-generated summaries to Silver data.

This separates ML inference from the core data pipeline:
- Transform DAGs stay fast and reliable
- ML can take its time (30-60s per mountain)
- If summarization fails, transforms still work
- Can be disabled independently

Schedule: Runs after daily transforms complete (12:30 PM UTC / 7:30 AM EST)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from summarize_snow_report import summarize_report
import json
import os
import boto3
from botocore.client import Config


# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def create_minio_client():
    """Create MinIO S3 client"""
    return boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id=os.getenv('MINIO_ROOT_USER'),
        aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD'),
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )


def add_summary_to_silver(mountain: str, **context):
    """
    Read Silver data, generate summary, update Silver
    
    Args:
        mountain: Mountain ID (cannon, bretton-woods, cranmore)
    """
    print("\n" + "="*60)
    print(f"AI SUMMARIZATION: {mountain.upper()}")
    print("="*60)
    
    s3_client = create_minio_client()
    
    # Get current Silver data
    try:
        response = s3_client.get_object(
            Bucket='silver-snow-reports',
            Key=f'{mountain}/current.json'
        )
        
        silver_data = json.loads(response['Body'].read().decode('utf-8'))
        print(f"Loaded Silver data for {mountain}")
        
    except Exception as e:
        print(f"Error loading Silver data: {e}")
        raise
    
    # Get narrative report
    narrative_report = silver_data.get('narrative_report', '')
    
    if not narrative_report:
        print("No narrative report found, skipping summarization")
        return
    
    print(f"Original report: {len(narrative_report.split())} words")
    
    # Generate summary
    print("\nGenerating DistilBART summary...")
    print("(This may take 30-60 seconds for first run)")
    
    try:
        summary = summarize_report(narrative_report, max_summary_words=100)
        print(f"Summary generated: {len(summary.split())} words")
        
    except Exception as e:
        print(f"Summarization failed: {e}")
        print("Using original text as fallback")
        summary = narrative_report
    
    # Update Silver data
    silver_data['narrative_summary'] = summary
    silver_data['summary_generated_at'] = datetime.now().isoformat()
    
    # Save back to Silver
    try:
        s3_client.put_object(
            Bucket='silver-snow-reports',
            Key=f'{mountain}/current.json',
            Body=json.dumps(silver_data, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
        
        print(f"Updated Silver with AI summary")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"Error saving to Silver: {e}")
        raise


# Create DAG
dag = DAG(
    'summarize_snow_reports',
    default_args=default_args,
    description='Generate AI summaries for snow reports',
    schedule_interval='30 12 * * *',  # 12:30 PM UTC (7:30 AM EST) - after transforms
    catchup=False,
    tags=['ai', 'nlp', 'silver']
)


# Create tasks for each mountain
with dag:
    summarize_cannon = PythonOperator(
        task_id='summarize_cannon',
        python_callable=add_summary_to_silver,
        op_kwargs={'mountain': 'cannon'}
    )
    
    summarize_bretton = PythonOperator(
        task_id='summarize_bretton_woods',
        python_callable=add_summary_to_silver,
        op_kwargs={'mountain': 'bretton-woods'}
    )
    
    summarize_cranmore = PythonOperator(
        task_id='summarize_cranmore',
        python_callable=add_summary_to_silver,
        op_kwargs={'mountain': 'cranmore'}
    )
    
    # Run SERIALLY - one mountain at a time to avoid overwhelming CPU
    summarize_cannon >> summarize_bretton >> summarize_cranmore