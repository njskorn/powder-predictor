"""
Visual Crossing Weather Scraper DAG
====================================

Daily weather data collection for all three mountains.

Fetches:
- Historical: Yesterday (to confirm actual conditions)
- Current: Real-time conditions  
- Forecast: Next 15 days

Schedule: Daily at 12:00 PM UTC (7:00 AM EST)
Runs at same time as mountain scrapers
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import json
import os
import boto3
from botocore.client import Config
import time


# DAG configuration
default_args = {
    'owner': 'nettle',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Mountain coordinates
MOUNTAINS = {
    'cannon': {
        'lat': 44.1574,
        'lon': -71.7013,
        'name': 'Cannon Mountain',
        'location': 'Franconia, NH'
    },
    'bretton-woods': {
        'lat': 44.2625,
        'lon': -71.4389,
        'name': 'Bretton Woods',
        'location': 'Bretton Woods, NH'
    },
    'cranmore': {
        'lat': 44.0543,
        'lon': -71.1281,
        'name': 'Cranmore Mountain',
        'location': 'North Conway, NH'
    }
}

BASE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"


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


def fetch_weather_data(lat, lon, start_date=None, end_date=None, include_forecast=True):
    """
    Fetch weather data from Visual Crossing
    
    Args:
        lat: Latitude
        lon: Longitude
        start_date: Start date (YYYY-MM-DD) for historical data
        end_date: End date (YYYY-MM-DD) for historical data
        include_forecast: Include forecast data
        
    Returns:
        Weather data dict or None on error
    """
    # Get API key from Airflow Variable
    api_key = Variable.get('visual_crossing_api_key')
    
    # Build URL
    if start_date and end_date:
        url = f"{BASE_URL}/{lat},{lon}/{start_date}/{end_date}"
    else:
        url = f"{BASE_URL}/{lat},{lon}"
    
    # Parameters
    params = {
        'key': api_key,
        'unitGroup': 'us',
        'include': 'days,current' if include_forecast else 'days',
        'elements': ','.join([
            'datetime', 'tempmax', 'tempmin', 'temp', 'feelslikemax', 'feelslikemin',
            'feelslike', 'dew', 'humidity', 'precip', 'precipprob', 'precipcover',
            'preciptype', 'snow', 'snowdepth', 'windgust', 'windspeed', 'winddir',
            'pressure', 'cloudcover', 'visibility', 'solarradiation', 'solarenergy',
            'uvindex', 'conditions', 'description', 'icon'
        ]),
        'contentType': 'json'
    }
    
    try:
        print(f"Fetching weather from: {url}")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        print(f"Retrieved {len(data.get('days', []))} days of data")
        
        return data
        
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        if e.response.status_code == 429:
            raise Exception("Rate limit exceeded")
        elif e.response.status_code == 401:
            raise Exception("Invalid API key")
        raise
        
    except Exception as e:
        print(f"Error: {e}")
        raise


def scrape_and_save_weather(**context):
    """
    Main scraper function - fetches weather for all mountains
    """
    print("="*60)
    print("VISUAL CROSSING WEATHER SCRAPER")
    print("="*60)
    print(f"Started: {datetime.now().isoformat()}")
    
    s3_client = create_minio_client()
    results = {}
    
    for mountain_id, mountain_data in MOUNTAINS.items():
        print(f"\n{'='*60}")
        print(f"SCRAPING: {mountain_data['name']}")
        print(f"{'='*60}")
        
        lat, lon = mountain_data['lat'], mountain_data['lon']
        
        weather_data = {
            'metadata': {
                'mountain': mountain_id,
                'mountain_name': mountain_data['name'],
                'location': mountain_data['location'],
                'coordinates': {'lat': lat, 'lon': lon},
                'scraped_at': datetime.now().isoformat(),
                'source': 'Visual Crossing Weather API',
                'scraper_version': '1.0'
            },
            'historical': None,
            'current': None,
            'forecast': None
        }
        
        try:
            # Step 1: Get historical data (yesterday only)
            print("\n[1/2] Getting historical data (yesterday)...")
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=1)
            
            historical_data = fetch_weather_data(
                lat, lon,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
                include_forecast=False
            )
            
            if historical_data:
                weather_data['historical'] = {
                    'query_dates': {
                        'start': start_date.strftime('%Y-%m-%d'),
                        'end': end_date.strftime('%Y-%m-%d')
                    },
                    'resolved_address': historical_data.get('resolvedAddress'),
                    'days': historical_data.get('days', [])
                }
                print(f"Retrieved {len(historical_data.get('days', []))} historical days")
            
            time.sleep(1)  # Rate limiting
            
            # Step 2: Get current + forecast
            print("\n[2/2] Getting current conditions + 15-day forecast...")
            
            forecast_data = fetch_weather_data(lat, lon, include_forecast=True)
            
            if forecast_data:
                # Extract current conditions
                if 'currentConditions' in forecast_data:
                    weather_data['current'] = forecast_data['currentConditions']
                    temp = weather_data['current'].get('temp')
                    conditions = weather_data['current'].get('conditions')
                    print(f"Current: {temp}°F, {conditions}")
                
                # Extract forecast
                if 'days' in forecast_data:
                    weather_data['forecast'] = {
                        'resolved_address': forecast_data.get('resolvedAddress'),
                        'timezone': forecast_data.get('timezone'),
                        'days': forecast_data['days']
                    }
                    print(f"Retrieved {len(forecast_data['days'])} forecast days")
            
            # Save to MinIO
            date_str = datetime.now().strftime('%Y-%m-%d')
            timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            key = f'weather/daily/{date_str}/{mountain_id}_{timestamp_str}.json'
            
            s3_client.put_object(
                Bucket='bronze-snow-reports',
                Key=key,
                Body=json.dumps(weather_data, indent=2).encode('utf-8'),
                ContentType='application/json',
                Metadata={
                    'mountain': mountain_id,
                    'scraped_at': weather_data['metadata']['scraped_at'],
                    'source': 'visual-crossing'
                }
            )
            
            print(f"Saved to MinIO: {key}")
            
            results[mountain_id] = {
                'success': True,
                'has_historical': weather_data['historical'] is not None,
                'has_current': weather_data['current'] is not None,
                'has_forecast': weather_data['forecast'] is not None
            }
            
        except Exception as e:
            print(f"Error for {mountain_id}: {e}")
            results[mountain_id] = {'success': False, 'error': str(e)}
            # Don't fail the whole DAG if one mountain fails
            continue
        
        # Wait between mountains
        if mountain_id != list(MOUNTAINS.keys())[-1]:
            print("\nWaiting 2 seconds before next mountain...")
            time.sleep(2)
    
    # Summary
    print("\n" + "="*60)
    print("SCRAPING COMPLETE")
    print("="*60)
    
    for mountain_id, result in results.items():
        if result.get('success'):
            print(f"{mountain_id}: Historical={result['has_historical']}, "
                  f"Current={result['has_current']}, Forecast={result['has_forecast']}")
        else:
            print(f"{mountain_id}: {result.get('error')}")
    
    print(f"\nFinished: {datetime.now().isoformat()}")
    print("="*60)
    
    # Fail if all mountains failed
    if not any(r.get('success') for r in results.values()):
        raise Exception("All mountain weather scrapes failed")


def ping_healthcheck(**context):
    """
    Ping Healthchecks.io after successful weather scrape
    """
    try:
        ping_url = Variable.get('healthchecks_weather_url')
        response = requests.get(ping_url, timeout=10)
        
        if response.status_code == 200:
            print("Successfully pinged Healthchecks.io")
        else:
            print(f"Healthchecks ping returned status {response.status_code}")
            
    except Exception as e:
        # Don't fail the DAG if healthcheck ping fails
        print(f"Could not ping Healthchecks.io: {e}")


# Define DAG
with DAG(
    dag_id='scrape_weather_visual_crossing',
    default_args=default_args,
    description='Daily weather scrape from Visual Crossing API',
    schedule='0 12 * * *',  # Run daily at 12:00 PM UTC (7:00 AM EST)
    start_date=datetime(2026, 2, 28),
    catchup=False,
    tags=['scraper', 'weather', 'visual-crossing', 'bronze'],
) as dag:
    
    scrape_task = PythonOperator(
        task_id='scrape_weather',
        python_callable=scrape_and_save_weather,
    )
    
    healthcheck_task = PythonOperator(
        task_id='ping_healthcheck',
        python_callable=ping_healthcheck,
    )
    
    # Dependencies
    scrape_task >> healthcheck_task