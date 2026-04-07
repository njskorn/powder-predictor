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
from botocore.config import Config

boto_config = Config(
    connect_timeout=5,
    read_timeout=10,
    retries={'max_attempts': 2}
)

s3_client = boto3.client(
    's3',
    endpoint_url=f'http{"s" if USE_SSL else ""}://{MINIO_ENDPOINT}',
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name='us-east-1',
    config=boto_config
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


def get_historical_reports(mountain: str, days: int = 30) -> List[Dict]:
    """
    Get historical terrain data from Silver layer daily files
    
    Reads pre-computed terrain counts from Silver layer, avoiding 
    duplicate work since Bronze→Silver transformation already did the counting
    
    Args:
        mountain: Mountain name
        days: Number of days to retrieve
        
    Returns:
        List of daily terrain counts for charting
    """
    from datetime import datetime, timedelta
    
    history = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    logger.info(f"Fetching {days} days of history for {mountain} from Silver layer")
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        prefix = f"{mountain}/daily/{date_str}/"
        
        logger.info(f"Checking {date_str} for {mountain}...")
        
        try:
            # List Silver files for this date
            logger.debug(f"Listing objects with prefix: {prefix}")
            response = s3_client.list_objects_v2(
                Bucket=SILVER_BUCKET,
                Prefix=prefix,
                MaxKeys=1  # Just get the first (most recent) file for this day
            )
            logger.debug(f"List response received for {date_str}")
            
            if 'Contents' in response and len(response['Contents']) > 0:
                # Get the most recent Silver file for this day
                key = response['Contents'][0]['Key']
                logger.debug(f"Found file: {key}")
                
                obj_response = s3_client.get_object(
                    Bucket=SILVER_BUCKET,
                    Key=key
                )
                content = obj_response['Body'].read().decode('utf-8')
                silver_data = json.loads(content)
                
                # Extract pre-computed terrain counts from Silver
                trails = silver_data.get('summary', {}).get('trails', {})
                by_diff = trails.get('by_difficulty', {})
                
                green = by_diff.get('green', {}).get('open', 0)
                blue = by_diff.get('blue', {}).get('open', 0)
                black = by_diff.get('black', {}).get('open', 0)
                glades = by_diff.get('glades', {}).get('open', 0)
                total = trails.get('open', 0)
                
                # Skip days with broken difficulty data
                # Case 1: Total > 0 but ALL difficulties are zero
                # Case 2: Total > 0 but breakdown sum is way off (e.g., 98 total but only 23 in glades)
                breakdown_sum = green + blue + black + glades
                
                if total > 0:
                    if breakdown_sum == 0:
                        # All zeros - completely broken
                        logger.debug(f"Skipping {date_str} - all difficulties are zero")
                        current_date += timedelta(days=1)
                        continue
                    elif breakdown_sum < total * 0.5:
                        # Breakdown accounts for less than 50% of total - partially broken
                        logger.debug(f"Skipping {date_str} - breakdown ({breakdown_sum}) << total ({total})")
                        current_date += timedelta(days=1)
                        continue
                
                history.append({
                    'date': date_str,
                    'green': green,
                    'blue': blue,
                    'black': black,
                    'glades': glades,
                    'total': total,
                    'is_closed': silver_data.get('is_closed', False)
                })
                logger.info(f"Added data for {date_str}")
            else:
                logger.debug(f"No files found for {date_str}")
                
        except Exception as e:
            logger.warning(f"Error for {date_str}: {e}")
            
        current_date += timedelta(days=1)
    
    logger.info(f"Retrieved {len(history)} days of data for {mountain}")
    return history


def get_weather_history(mountain: str, days: int = 30) -> List[Dict]:
    """
    Get historical weather data from Silver layer
    
    Reads weather from Silver: weather/{mountain}/daily/{date}.json
    
    Args:
        mountain: Mountain name
        days: Number of days to retrieve
        
    Returns:
        List of daily weather data for charting
    """
    from datetime import datetime, timedelta
    
    history = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    logger.info(f"Fetching {days} days of weather history for {mountain} from Silver layer")
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        key = f"weather/{mountain}/daily/{date_str}.json"
        
        try:
            # Get weather file for this date
            response = s3_client.get_object(
                Bucket=SILVER_BUCKET,
                Key=key
            )
            
            content = response['Body'].read().decode('utf-8')
            weather_data = json.loads(content)
            
            # Extract actual weather (not forecasts)
            actual = weather_data.get('actual', {})
            
            # Build chart-friendly data structure
            history.append({
                'date': date_str,
                'temp_max': actual.get('temperature', {}).get('max'),
                'temp_min': actual.get('temperature', {}).get('min'),
                'temp_avg': actual.get('temperature', {}).get('avg'),
                'feels_like_max': actual.get('feels_like', {}).get('max'),
                'feels_like_min': actual.get('feels_like', {}).get('min'),
                'snow': actual.get('snow', {}).get('new_snow'),
                'snowdepth': actual.get('snow', {}).get('depth'),
                'precip': actual.get('precipitation', {}).get('total'),
                'wind_speed': actual.get('wind', {}).get('speed'),
                'wind_gust': actual.get('wind', {}).get('gust'),
                'humidity': actual.get('conditions', {}).get('humidity'),
                'conditions': actual.get('conditions', {}).get('conditions'),
                'description': actual.get('conditions', {}).get('description')
            })
            
            logger.debug(f"✓ Added weather for {date_str}")
            
        except ClientError as e:
            # File doesn't exist for this date - that's okay, just skip
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.debug(f"No weather data for {date_str}")
            else:
                logger.warning(f"Error fetching weather for {date_str}: {e}")
                
        except Exception as e:
            logger.warning(f"Error processing weather for {date_str}: {e}")
            
        current_date += timedelta(days=1)
    
    logger.info(f"Retrieved {len(history)} days of weather data for {mountain}")
    return history


def get_powder_predictions(mountain: str, days: int = 7):
    """
    Get powder day predictions from Gold layer
    
    Args:
        mountain: Mountain ID
        days: Number of days ahead to retrieve
        
    Returns:
        List of powder predictions with dates and confidence
    """
    from datetime import datetime, timedelta
    
    s3_client = create_minio_client()
    predictions = []
    
    # Get predictions for next N days
    for i in range(days):
        target_date = (datetime.now() + timedelta(days=i)).strftime('%Y-%m-%d')
        
        try:
            # Try predictions first (future)
            response = s3_client.get_object(
                Bucket='gold-snow-reports',
                Key=f'powder_analysis/predictions/{mountain}/{target_date}.json'
            )
            analysis = json.loads(response['Body'].read())
            predictions.append(analysis)
            
        except:
            # Try historical (for showing past performance)
            try:
                response = s3_client.get_object(
                    Bucket='gold-snow-reports',
                    Key=f'powder_analysis/historical/{mountain}/{target_date}.json'
                )
                analysis = json.loads(response['Body'].read())
                predictions.append(analysis)
            except:
                pass
    
    return predictions


def get_historical_powder_days(mountain: str, days: int = 30):
    """
    Get historical powder day analyses
    
    Returns list of dates where powder was detected (TP or FN)
    """
    from datetime import datetime, timedelta
    
    s3_client = create_minio_client()
    powder_days = []
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    current = start_date
    while current <= end_date:
        date_str = current.strftime('%Y-%m-%d')
        
        try:
            response = s3_client.get_object(
                Bucket='gold-snow-reports',
                Key=f'powder_analysis/historical/{mountain}/{date_str}.json'
            )
            analysis = json.loads(response['Body'].read())
            
            # Include if actual powder day OR if we predicted powder
            if analysis.get('actual', {}).get('was_powder_day') or \
               analysis.get('prediction', {}).get('is_powder_day'):
                powder_days.append(analysis)
                
        except:
            pass
        
        current += timedelta(days=1)
    
    return powder_days