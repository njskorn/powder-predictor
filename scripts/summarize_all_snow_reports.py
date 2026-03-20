"""
Run AI Summarization Locally
=============================

Runs outside Docker using your local machine's full resources.
Updates Silver data with AI summaries.

Usage:
    python summarize_all_mountains.py
"""

from summarize_snow_report import summarize_report
import json
import os
from datetime import datetime
import boto3
from botocore.client import Config


MOUNTAINS = ['cannon', 'bretton-woods', 'cranmore']


def create_minio_client():
    """Create MinIO S3 client"""
    return boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id=os.getenv('MINIO_ROOT_USER', 'minioadmin'),
        aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD', 'minio_secure_2025'),
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )


def summarize_mountain(s3_client, mountain: str):
    """Generate and save summary for one mountain"""
    
    print(f"\n{'='*60}")
    print(f"SUMMARIZING: {mountain.upper()}")
    print(f"{'='*60}")
    
    # Load Silver data
    try:
        response = s3_client.get_object(
            Bucket='silver-snow-reports',
            Key=f'{mountain}/current.json'
        )
        
        silver_data = json.loads(response['Body'].read().decode('utf-8'))
        print(f"Loaded Silver data")
        
    except Exception as e:
        print(f"Error loading: {e}")
        return False
    
    # Get narrative
    narrative_report = silver_data.get('narrative_report', '')
    
    if not narrative_report:
        print("No narrative report found, skipping")
        return False
    
    print(f"Original: {len(narrative_report.split())} words")
    
    # Generate summary
    print("Generating DistilBART summary...")
    
    try:
        summary = summarize_report(narrative_report, max_summary_words=100)
        print(f"Summary: {len(summary.split())} words")
        print(summary)
        
    except Exception as e:
        print(f"Summarization failed: {e}")
        summary = narrative_report
    
    # Update Silver
    silver_data['narrative_summary'] = summary
    silver_data['summary_generated_at'] = datetime.now().isoformat()
    
    # Save
    try:
        s3_client.put_object(
            Bucket='silver-snow-reports',
            Key=f'{mountain}/current.json',
            Body=json.dumps(silver_data, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
        
        print(f"Updated Silver")
        return True
        
    except Exception as e:
        print(f"Error saving: {e}")
        return False


def main():
    """Summarize all mountains"""
    
    print("\n" + "="*60)
    print("AI SNOW REPORT SUMMARIZATION")
    print("="*60)
    print(f"Mountains: {', '.join(MOUNTAINS)}")
    print(f"Running locally (not in Docker)")
    print("="*60)
    
    s3_client = create_minio_client()
    
    success_count = 0
    
    for mountain in MOUNTAINS:
        if summarize_mountain(s3_client, mountain):
            success_count += 1
    
    print("\n" + "="*60)
    print("COMPLETE")
    print("="*60)
    print(f"Successful: {success_count}/{len(MOUNTAINS)}")
    print("="*60 + "\n")


if __name__ == '__main__':
    main()