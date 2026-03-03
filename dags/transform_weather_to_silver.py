"""
Weather Transformation DAG (Bronze → Silver)
=============================================

Daily transformation of weather data from Bronze to Silver layer.

Transforms:
- Yesterday's weather (actual conditions)
- Today's weather (current + forecast)
- Collects historical forecasts for validation

Runs 15 after scrape_weather_visual_crossing
"""

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import boto3
from botocore.client import Config
import os


default_args = {
    'owner': 'nettle',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

MOUNTAINS = ['cannon', 'bretton-woods', 'cranmore']


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


def get_forecasts_for_date(s3_client, mountain_id, target_date):
    """
    Collect all historical forecasts for this date
    
    Looks back up to 15 days in Bronze to find forecasts
    """
    forecasts = []
    target = datetime.strptime(target_date, '%Y-%m-%d')
    
    # Look back up to 15 days (max forecast range)
    for days_back in range(1, 16):
        forecast_date = target - timedelta(days=days_back)
        forecast_date_str = forecast_date.strftime('%Y-%m-%d')
        
        prefix = f'weather/daily/{forecast_date_str}/'
        
        try:
            response = s3_client.list_objects_v2(
                Bucket='bronze-snow-reports',
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                continue
            
            # Filter for this mountain, get most recent
            mountain_files = [
                obj for obj in response['Contents']
                if mountain_id in obj['Key']
            ]
            
            if not mountain_files:
                continue
            
            mountain_files.sort(key=lambda x: x['LastModified'], reverse=True)
            latest_key = mountain_files[0]['Key']
            
            # Load file and look for forecast
            obj = s3_client.get_object(
                Bucket='bronze-snow-reports',
                Key=latest_key
            )
            
            data = json.loads(obj['Body'].read().decode('utf-8'))
            
            if data.get('forecast', {}).get('days'):
                for day in data['forecast']['days']:
                    if day.get('datetime') == target_date:
                        forecasts.append({
                            'predicted_on': forecast_date_str,
                            'days_ahead': days_back,
                            'predicted_temp_max': day.get('tempmax'),
                            'predicted_temp_min': day.get('tempmin'),
                            'predicted_snow': day.get('snow'),
                            'predicted_precip': day.get('precip'),
                            'predicted_precip_prob': day.get('precipprob'),
                            'predicted_conditions': day.get('conditions'),
                            'bronze_source': latest_key
                        })
                        break
        
        except Exception as e:
            continue
    
    forecasts.sort(key=lambda x: x['predicted_on'])
    return forecasts


def transform_day(s3_client, mountain_id, date_str):
    """Transform weather for a single day"""
    
    # Get Bronze files for this date
    prefix = f'weather/daily/{date_str}/'
    
    try:
        response = s3_client.list_objects_v2(
            Bucket='bronze-snow-reports',
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            return None
        
        # Filter for this mountain
        mountain_files = [
            obj for obj in response['Contents']
            if mountain_id in obj['Key']
        ]
        
        if not mountain_files:
            return None
        
        # Get most recent file
        mountain_files.sort(key=lambda x: x['LastModified'], reverse=True)
        latest = mountain_files[0]
        
        obj = s3_client.get_object(
            Bucket='bronze-snow-reports',
            Key=latest['Key']
        )
        
        data = json.loads(obj['Body'].read().decode('utf-8'))
        
        # Find historical data for this date
        historical = data.get('historical', {})
        days = historical.get('days', [])
        
        day_data = None
        for day in days:
            if day.get('datetime') == date_str:
                day_data = day
                break
        
        if not day_data:
            return None
        
        # Build Silver structure
        silver = {
            'date': date_str,
            'mountain': mountain_id,
            'source': 'Visual Crossing Weather API',
            'processed_at': datetime.now().isoformat(),
            'bronze_source': latest['Key'],
            'actual': {
                'temperature': {
                    'max': day_data.get('tempmax'),
                    'min': day_data.get('tempmin'),
                    'avg': day_data.get('temp'),
                    'unit': 'F'
                },
                'feels_like': {
                    'max': day_data.get('feelslikemax'),
                    'min': day_data.get('feelslikemin'),
                    'avg': day_data.get('feelslike'),
                    'unit': 'F'
                },
                'precipitation': {
                    'total': day_data.get('precip'),
                    'probability': day_data.get('precipprob'),
                    'type': day_data.get('preciptype'),
                    'unit': 'inches'
                },
                'snow': {
                    'new_snow': day_data.get('snow'),
                    'depth': day_data.get('snowdepth'),
                    'unit': 'inches'
                },
                'wind': {
                    'speed': day_data.get('windspeed'),
                    'gust': day_data.get('windgust'),
                    'direction': day_data.get('winddir'),
                    'unit': 'mph'
                },
                'conditions': {
                    'humidity': day_data.get('humidity'),
                    'pressure': day_data.get('pressure'),
                    'cloud_cover': day_data.get('cloudcover'),
                    'visibility': day_data.get('visibility'),
                    'description': day_data.get('description'),
                    'conditions': day_data.get('conditions'),
                    'icon': day_data.get('icon')
                }
            },
            'forecasts': []
        }
        
        # Collect forecasts for this date
        forecasts = get_forecasts_for_date(s3_client, mountain_id, date_str)
        silver['forecasts'] = forecasts
        
        return silver
        
    except Exception as e:
        print(f"Error transforming {mountain_id} {date_str}: {e}")
        return None


def transform_current(s3_client, mountain_id):
    """Create current.json with latest conditions + forecast"""
    
    today = datetime.now().strftime('%Y-%m-%d')
    prefix = f'weather/daily/{today}/'
    
    try:
        response = s3_client.list_objects_v2(
            Bucket='bronze-snow-reports',
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            return None
        
        # Filter for this mountain, get most recent
        mountain_files = [
            obj for obj in response['Contents']
            if mountain_id in obj['Key']
        ]
        
        if not mountain_files:
            return None
        
        mountain_files.sort(key=lambda x: x['LastModified'], reverse=True)
        latest_key = mountain_files[0]['Key']
        
        obj = s3_client.get_object(
            Bucket='bronze-snow-reports',
            Key=latest_key
        )
        
        data = json.loads(obj['Body'].read().decode('utf-8'))
        
        # Build current.json
        silver = {
            'mountain': mountain_id,
            'updated_at': datetime.now().isoformat(),
            'source': 'Visual Crossing Weather API',
            'bronze_source': latest_key,
            'current_conditions': None,
            'forecast': []
        }
        
        # Current conditions
        if data.get('current'):
            current = data['current']
            silver['current_conditions'] = {
                'timestamp': current.get('datetime'),
                'temperature': current.get('temp'),
                'feels_like': current.get('feelslike'),
                'humidity': current.get('humidity'),
                'wind_speed': current.get('windspeed'),
                'wind_gust': current.get('windgust'),
                'wind_direction': current.get('winddir'),
                'conditions': current.get('conditions'),
                'visibility': current.get('visibility'),
                'pressure': current.get('pressure')
            }
        
        # Forecast (future only)
        if data.get('forecast', {}).get('days'):
            today_date = datetime.now().date()
            
            for day in data['forecast']['days']:
                day_date = datetime.strptime(day['datetime'], '%Y-%m-%d').date()
                
                if day_date > today_date:
                    silver['forecast'].append({
                        'date': day['datetime'],
                        'temp_max': day.get('tempmax'),
                        'temp_min': day.get('tempmin'),
                        'snow': day.get('snow'),
                        'precip': day.get('precip'),
                        'precip_prob': day.get('precipprob'),
                        'conditions': day.get('conditions'),
                        'description': day.get('description')
                    })
        
        return silver
        
    except Exception as e:
        print(f"Error creating current.json for {mountain_id}: {e}")
        return None


def transform_weather(**context):
    """
    Main transformation - process yesterday + today for all mountains
    """
    print("="*60)
    print("WEATHER TRANSFORMATION (Bronze → Silver)")
    print("="*60)
    
    s3_client = create_minio_client()
    
    # Transform yesterday + today
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    today = datetime.now().strftime('%Y-%m-%d')
    
    dates = [yesterday, today]
    
    results = {}
    
    for mountain_id in MOUNTAINS:
        print(f"\nTransforming: {mountain_id}")
        results[mountain_id] = {'daily': 0, 'current': False}
        
        # Transform daily files
        for date_str in dates:
            silver_data = transform_day(s3_client, mountain_id, date_str)
            
            if silver_data:
                key = f'weather/{mountain_id}/daily/{date_str}.json'
                
                s3_client.put_object(
                    Bucket='silver-snow-reports',
                    Key=key,
                    Body=json.dumps(silver_data, indent=2).encode('utf-8'),
                    ContentType='application/json',
                    Metadata={
                        'mountain': mountain_id,
                        'date': date_str,
                        'has_actual': 'true',
                        'forecast_count': str(len(silver_data.get('forecasts', [])))
                    }
                )
                
                results[mountain_id]['daily'] += 1
                print(f"  {date_str}: {len(silver_data.get('forecasts', []))} forecasts collected")
        
        # Update current.json
        current_data = transform_current(s3_client, mountain_id)
        
        if current_data:
            s3_client.put_object(
                Bucket='silver-snow-reports',
                Key=f'weather/{mountain_id}/current.json',
                Body=json.dumps(current_data, indent=2).encode('utf-8'),
                ContentType='application/json'
            )
            
            results[mountain_id]['current'] = True
            print(f"  current.json: {len(current_data.get('forecast', []))} forecast days")
    
    # Summary
    print("\n" + "="*60)
    print("TRANSFORMATION COMPLETE")
    print("="*60)
    
    for mountain_id, result in results.items():
        print(f"{mountain_id}: {result['daily']} daily files, current={result['current']}")
    
    print("="*60)


# Define DAG
with DAG(
    dag_id='transform_weather',
    default_args=default_args,
    description='Transform weather data from Bronze to Silver',
    schedule='15 12 * * *',  # 15 minutes after weather scraper
    start_date=datetime(2026, 2, 28),
    catchup=False,
    tags=['transformation', 'weather', 'silver'],
) as dag:
    
    transform_task = PythonOperator(
        task_id='transform_weather',
        python_callable=transform_weather,
    )