#!/usr/bin/env python3
"""
NOAA Weather Scraper
====================

Fetches weather data from NOAA API for all three mountains.
No API key required - NOAA data is public.

Data collected:
- Current conditions (temp, wind, precipitation)
- 7-day forecast
- Observation station metadata
"""

import requests
import json
import os
from datetime import datetime
from typing import Dict, Any, Optional
import time


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

# NOAA API base URL
NOAA_BASE = 'https://api.weather.gov'

# Required headers for NOAA API (they request a User-Agent)
HEADERS = {
    'User-Agent': '(Powder Predictor, nettle@example.com)',
    'Accept': 'application/json'
}


def get_gridpoint_data(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    """
    Get NOAA gridpoint metadata for coordinates
    
    This returns URLs for forecast and observation station
    
    Args:
        lat: Latitude
        lon: Longitude
        
    Returns:
        Dict with forecast URL and observation stations
    """
    url = f'{NOAA_BASE}/points/{lat},{lon}'
    
    try:
        print(f"   Fetching gridpoint data: {url}")
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        return {
            'forecast_url': data['properties']['forecast'],
            'forecast_hourly_url': data['properties']['forecastHourly'],
            'observation_stations_url': data['properties']['observationStations'],
            'grid_id': data['properties']['gridId'],
            'grid_x': data['properties']['gridX'],
            'grid_y': data['properties']['gridY']
        }
        
    except Exception as e:
        print(f"Error fetching gridpoint: {e}")
        return None


def get_observation_station(stations_url: str) -> Optional[str]:
    """
    Get the nearest observation station
    
    Args:
        stations_url: URL to observation stations list
        
    Returns:
        Station ID for observations
    """
    try:
        print(f"   Fetching observation stations: {stations_url}")
        response = requests.get(stations_url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        # Get first (nearest) station
        if data['features'] and len(data['features']) > 0:
            station_url = data['features'][0]['id']
            station_id = station_url.split('/')[-1]
            print(f"   Using station: {station_id}")
            return station_url
        
        return None
        
    except Exception as e:
        print(f"Error fetching stations: {e}")
        return None


def get_current_observations(station_url: str) -> Optional[Dict[str, Any]]:
    """
    Get current weather observations from station
    
    Args:
        station_url: Station URL from get_observation_station
        
    Returns:
        Current conditions data
    """
    try:
        url = f'{station_url}/observations/latest'
        print(f"   Fetching current observations: {url}")
        
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        props = data['properties']
        
        # Extract key observations
        observations = {
            'timestamp': props.get('timestamp'),
            'temperature': {
                'value': props.get('temperature', {}).get('value'),
                'unit': props.get('temperature', {}).get('unitCode', '').split(':')[-1]
            },
            'dewpoint': {
                'value': props.get('dewpoint', {}).get('value'),
                'unit': props.get('dewpoint', {}).get('unitCode', '').split(':')[-1]
            },
            'wind_direction': {
                'value': props.get('windDirection', {}).get('value'),
                'unit': 'degrees'
            },
            'wind_speed': {
                'value': props.get('windSpeed', {}).get('value'),
                'unit': props.get('windSpeed', {}).get('unitCode', '').split(':')[-1]
            },
            'wind_gust': {
                'value': props.get('windGust', {}).get('value'),
                'unit': props.get('windGust', {}).get('unitCode', '').split(':')[-1]
            },
            'barometric_pressure': {
                'value': props.get('barometricPressure', {}).get('value'),
                'unit': props.get('barometricPressure', {}).get('unitCode', '').split(':')[-1]
            },
            'visibility': {
                'value': props.get('visibility', {}).get('value'),
                'unit': props.get('visibility', {}).get('unitCode', '').split(':')[-1]
            },
            'relative_humidity': {
                'value': props.get('relativeHumidity', {}).get('value'),
                'unit': '%'
            },
            'wind_chill': {
                'value': props.get('windChill', {}).get('value'),
                'unit': props.get('windChill', {}).get('unitCode', '').split(':')[-1]
            },
            'precipitation_last_hour': {
                'value': props.get('precipitationLastHour', {}).get('value'),
                'unit': props.get('precipitationLastHour', {}).get('unitCode', '').split(':')[-1]
            },
            'text_description': props.get('textDescription'),
            'icon': props.get('icon'),
            'raw_message': props.get('rawMessage')
        }
        
        print(f"   Temperature: {observations['temperature']['value']}°C")
        print(f"   Wind: {observations['wind_speed']['value']} {observations['wind_speed']['unit']}")
        
        return observations
        
    except Exception as e:
        print(f"Error fetching observations: {e}")
        return None


def get_forecast(forecast_url: str) -> Optional[Dict[str, Any]]:
    """
    Get 7-day forecast
    
    Args:
        forecast_url: Forecast URL from gridpoint
        
    Returns:
        Forecast periods data
    """
    try:
        print(f"   Fetching forecast: {forecast_url}")
        
        response = requests.get(forecast_url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract forecast periods (usually 14 periods = 7 days, day+night)
        periods = []
        for period in data['properties']['periods']:
            periods.append({
                'number': period.get('number'),
                'name': period.get('name'),
                'start_time': period.get('startTime'),
                'end_time': period.get('endTime'),
                'is_daytime': period.get('isDaytime'),
                'temperature': period.get('temperature'),
                'temperature_unit': period.get('temperatureUnit'),
                'temperature_trend': period.get('temperatureTrend'),
                'wind_speed': period.get('windSpeed'),
                'wind_direction': period.get('windDirection'),
                'icon': period.get('icon'),
                'short_forecast': period.get('shortForecast'),
                'detailed_forecast': period.get('detailedForecast')
            })
        
        print(f"Retrieved {len(periods)} forecast periods")
        
        return {
            'updated': data['properties']['updated'],
            'periods': periods
        }
        
    except Exception as e:
        print(f"Error fetching forecast: {e}")
        return None


def scrape_mountain_weather(mountain_id: str, mountain_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Scrape complete weather data for one mountain
    
    Args:
        mountain_id: Mountain identifier (e.g., 'cannon')
        mountain_data: Mountain metadata with coordinates
        
    Returns:
        Complete weather data structure
    """
    print(f"\n{'='*60}")
    print(f"SCRAPING WEATHER: {mountain_data['name']}")
    print(f"{'='*60}")
    
    weather_data = {
        'metadata': {
            'mountain': mountain_id,
            'mountain_name': mountain_data['name'],
            'location': mountain_data['location'],
            'coordinates': {
                'lat': mountain_data['lat'],
                'lon': mountain_data['lon']
            },
            'scraped_at': datetime.now().isoformat(),
            'source': 'NOAA Weather Service API',
            'scraper_version': '1.0'
        },
        'gridpoint': None,
        'current_conditions': None,
        'forecast': None
    }
    
    # Step 1: Get gridpoint data
    print("\n[1/3] Getting gridpoint metadata...")
    gridpoint = get_gridpoint_data(mountain_data['lat'], mountain_data['lon'])
    
    if not gridpoint:
        print("Failed to get gridpoint data")
        return weather_data
    
    weather_data['gridpoint'] = gridpoint
    time.sleep(0.5)  # Be nice to NOAA servers
    
    # Step 2: Get current observations
    print("\n[2/3] Getting current conditions...")
    station_url = get_observation_station(gridpoint['observation_stations_url'])
    
    if station_url:
        time.sleep(0.5)
        observations = get_current_observations(station_url)
        weather_data['current_conditions'] = observations
    else:
        print("No observation station found")
    
    # Step 3: Get forecast
    print("\n[3/3] Getting 7-day forecast...")
    time.sleep(0.5)
    forecast = get_forecast(gridpoint['forecast_url'])
    weather_data['forecast'] = forecast
    
    print(f"\n{'='*60}")
    print(f"COMPLETED: {mountain_data['name']}")
    print(f"{'='*60}")
    
    return weather_data


def save_to_minio(mountain_id: str, weather_data: Dict[str, Any]) -> bool:
    """
    Save weather data to MinIO Bronze layer
    
    Args:
        mountain_id: Mountain identifier
        weather_data: Weather data to save
        
    Returns:
        True if successful
    """
    import boto3
    from botocore.client import Config
    
    try:
        # Create S3 client
        s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id=os.getenv('MINIO_ROOT_USER', 'minioadmin'),
            aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD', 'minio_secure_2025'),
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        # Create path: weather/daily/YYYY-MM-DD/mountain_HHMMSS.json
        date_str = datetime.now().strftime('%Y-%m-%d')
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        key = f'weather/daily/{date_str}/{mountain_id}_{timestamp_str}.json'
        
        # Save to Bronze bucket
        s3_client.put_object(
            Bucket='bronze-snow-reports',
            Key=key,
            Body=json.dumps(weather_data, indent=2).encode('utf-8'),
            ContentType='application/json',
            Metadata={
                'mountain': mountain_id,
                'scraped_at': weather_data['metadata']['scraped_at'],
                'source': 'noaa'
            }
        )
        
        print(f"✓ Saved to MinIO: bronze-snow-reports/{key}")
        return True
        
    except Exception as e:
        print(f"✗ Error saving to MinIO: {e}")
        return False


def main():
    """
    Main scraper - fetch weather for all mountains
    """
    print("\n" + "="*60)
    print("NOAA WEATHER SCRAPER")
    print("="*60)
    print(f"Started: {datetime.now().isoformat()}")
    print(f"Mountains: {len(MOUNTAINS)}")
    
    results = {}
    
    for mountain_id, mountain_data in MOUNTAINS.items():
        # Scrape weather
        weather_data = scrape_mountain_weather(mountain_id, mountain_data)
        
        # Save to MinIO
        success = save_to_minio(mountain_id, weather_data)
        
        results[mountain_id] = {
            'success': success,
            'has_current': weather_data['current_conditions'] is not None,
            'has_forecast': weather_data['forecast'] is not None
        }
        
        # Wait between mountains to be respectful
        if mountain_id != list(MOUNTAINS.keys())[-1]:  # Not last mountain
            print("\nWaiting 2 seconds before next mountain...")
            time.sleep(2)
    
    # Summary
    print("\n" + "="*60)
    print("SCRAPING COMPLETE")
    print("="*60)
    
    for mountain_id, result in results.items():
        status = "✓" if result['success'] else "✗"
        print(f"{status} {mountain_id}: Current={result['has_current']}, Forecast={result['has_forecast']}")
    
    print(f"\nFinished: {datetime.now().isoformat()}")
    print("="*60 + "\n")


if __name__ == '__main__':
    main()