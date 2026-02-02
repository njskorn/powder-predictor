"""
MinIO Client Service
Handles reading Silver layer data from MinIO
"""

import json
import boto3
from botocore.exceptions import ClientError
import logging
from typing import Dict, List

from .config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    SILVER_BUCKET,
    USE_SSL,
    MOUNTAINS
)

logger = logging.getLogger(__name__)

# Initialize S3 client for MinIO
s3_client = boto3.client(
    's3',
    endpoint_url=f'http{"s" if USE_SSL else ""}://{MINIO_ENDPOINT}',
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name='us-east-1'
)


def get_current_report(mountain: str) -> Dict:
    """
    Get the current.json Silver layer report for a mountain
    
    Args:
        mountain: Mountain name (bretton-woods, cannon, or cranmore)
        
    Returns:
        Dictionary containing the Silver layer report
        
    Raises:
        FileNotFoundError: If current.json doesn't exist
        ValueError: If JSON is invalid
    """
    try:
        key = f"{mountain}/current.json"
        logger.info(f"Fetching s3://{SILVER_BUCKET}/{key}")
        
        response = s3_client.get_object(
            Bucket=SILVER_BUCKET,
            Key=key
        )
        
        # Read and parse JSON
        data = response['Body'].read().decode('utf-8')
        report = json.loads(data)
        
        logger.info(f"Successfully loaded {mountain} report")
        return report
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            logger.error(f"No current.json found for {mountain}")
            raise FileNotFoundError(f"No data available for {mountain}")
        else:
            logger.error(f"MinIO error: {e}")
            raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON for {mountain}: {e}")
        raise ValueError(f"Invalid data format for {mountain}")


def list_mountains() -> List[str]:
    """
    Get list of available mountains
    
    Returns:
        List of mountain names that have current.json files
    """
    available = []
    
    for mountain in MOUNTAINS:
        try:
            # Check if current.json exists
            s3_client.head_object(
                Bucket=SILVER_BUCKET,
                Key=f"{mountain}/current.json"
            )
            available.append(mountain)
        except ClientError:
            logger.warning(f"No current.json found for {mountain}")
    
    return available


def check_minio_connection() -> bool:
    """
    Test MinIO connection
    
    Returns:
        True if connection successful, False otherwise
    """
    try:
        s3_client.list_buckets()
        logger.info("MinIO connection successful")
        return True
    except Exception as e:
        logger.error(f"MinIO connection failed: {e}")
        return False