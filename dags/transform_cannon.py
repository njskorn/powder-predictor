"""
Cannon Mountain: Bronze → Silver Transformation
===============================================

Transforms raw Cannon scrape data into standardized Silver layer format
Cannon has BASE and SUMMIT weather data
"""

from datetime import datetime
from typing import Dict, Any
from silver_utils import (
    parse_fraction, calculate_percent, parse_lift_capacity,
    standardize_difficulty, is_groomed, parse_snowfall, parse_temperature,
    calculate_age_minutes, count_by_difficulty, create_weather_field,
    create_summary_metric
)

def transform_cannon(bronze_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform Cannon Mountain Bronze layer data to Silver layer
    
    Args:
        bronze_data: Raw Bronze layer JSON from scraper
        
    Returns:
        Standardized Silver layer JSON
    """
    
    processed_at = datetime.now().isoformat()
    source_scraped_at = bronze_data['metadata'].get('scraped_at', processed_at)
    
    # Initialize Silver structure
    silver = {
        'mountain': 'cannon',
        'silver_version': '1.0',
        'processed_at': processed_at,
        'source_scraped_at': source_scraped_at,
        'data_freshness': {
            'age_minutes': calculate_age_minutes(source_scraped_at, processed_at),
            'is_stale': False
        },
        'summary': {
            'lifts': {},
            'trails': {},
            'glades': {}
        },
        'weather': {},
        'lifts': [],
        'trails': [],
        'glades': []
    }
    
    # Set stale flag
    silver['data_freshness']['is_stale'] = silver['data_freshness']['age_minutes'] > 120
    
    # ========================================================================
    # SECTION 1: Summary Metrics
    # ========================================================================
    
    # Cannon has nested structure: summary_metrics.mountain_report_page
    report_metrics = bronze_data.get('summary_metrics', {}).get('mountain_report_page', {})
    
    # Lifts - reported as number, need to count total
    lifts_open_str = report_metrics.get('open_lifts')
    if lifts_open_str:
        try:
            lifts_open = int(lifts_open_str)
            # Count total from lifts array
            lifts_total = len(bronze_data.get('lifts', []))
            silver['summary']['lifts'] = create_summary_metric(lifts_open, lifts_total)
        except:
            pass
    
    # If that didn't work, count from arrays
    if not silver['summary']['lifts']:
        bronze_lifts = bronze_data.get('lifts', [])
        lifts_open = sum(1 for lift in bronze_lifts if lift.get('status') == 'open')
        lifts_total = len(bronze_lifts)
        silver['summary']['lifts'] = create_summary_metric(lifts_open, lifts_total)
    
    # Trails - same pattern
    trails_open_str = report_metrics.get('open_trails')
    if trails_open_str:
        try:
            trails_open = int(trails_open_str)
            trails_total = len(bronze_data.get('trails', []))
            silver['summary']['trails'] = create_summary_metric(trails_open, trails_total)
        except:
            pass
    
    if not silver['summary']['trails']:
        bronze_trails = bronze_data.get('trails', [])
        trails_open = sum(1 for trail in bronze_trails if trail.get('status') == 'open')
        trails_total = len(bronze_trails)
        silver['summary']['trails'] = create_summary_metric(trails_open, trails_total)
    
    # Add difficulty breakdown
    bronze_trails = bronze_data.get('trails', [])
    silver['summary']['trails']['by_difficulty'] = count_by_difficulty(bronze_trails)
    
    # Glades - count from glades array
    bronze_glades = bronze_data.get('glades', [])
    glades_open = sum(1 for glade in bronze_glades if glade.get('status') == 'open')
    glades_total = len(bronze_glades)
    silver['summary']['glades'] = create_summary_metric(glades_open, glades_total)
    
    # ========================================================================
    # SECTION 2: Weather (Cannon has BASE and SUMMIT!)
    # ========================================================================
    
    weather_base = bronze_data.get('weather', {}).get('base', {})
    
    # Temperature - prefer LOW for consistency across mountains
    temp_low = parse_temperature(weather_base.get('temperature_low'))
    if temp_low is not None:
        silver['weather']['temperature_base'] = create_weather_field(
            value=temp_low,
            unit='F',
            quality_note='using temperature_low for consistency'
        )
    else:
        # Fallback to HIGH if LOW not available
        temp_high = parse_temperature(weather_base.get('temperature_high'))
        if temp_high is not None:
            silver['weather']['temperature_base'] = create_weather_field(
                value=temp_high,
                unit='F',
                quality_note='using temperature_high (low not available)'
            )
        else:
            silver['weather']['temperature_base'] = create_weather_field(
                value=None,
                unit='F',
                available=False
            )
    
    # Recent snowfall - "new" means 24hr
    snowfall_new = parse_snowfall(report_metrics.get('snowfall_new'))
    if snowfall_new:
        silver['weather']['snowfall_recent'] = create_weather_field(
            value=snowfall_new,
            unit='24hr',
            quality_note='new snowfall assumed to be 24hr'
        )
    else:
        silver['weather']['snowfall_recent'] = create_weather_field(
            value=None,
            unit='24hr',
            available=False
        )
    
    # Season total
    snowfall_season = parse_snowfall(report_metrics.get('snowfall_to_date'))
    if snowfall_season:
        silver['weather']['snowfall_season'] = create_weather_field(
            value=snowfall_season,
            unit='inches'
        )
    else:
        silver['weather']['snowfall_season'] = create_weather_field(
            value=None,
            unit='inches',
            available=False
        )
    
    # ========================================================================
    # SECTION 3: Lifts (detailed)
    # ========================================================================
    
    for bronze_lift in bronze_data.get('lifts', []):
        capacity_info = parse_lift_capacity(bronze_lift.get('name', ''))
        
        silver_lift = {
            'name': bronze_lift.get('name'),
            'status': bronze_lift.get('status'),
            'capacity': capacity_info['capacity'],
            'type': capacity_info['type']
        }
        
        silver['lifts'].append(silver_lift)
    
    # ========================================================================
    # SECTION 4: Trails (detailed)
    # ========================================================================
    
    for bronze_trail in bronze_data.get('trails', []):
        silver_trail = {
            'name': bronze_trail.get('name'),
            'status': bronze_trail.get('status'),
            'area': bronze_trail.get('area'),
            'difficulty': standardize_difficulty(bronze_trail.get('difficulty')),
            'groomed': is_groomed(bronze_trail.get('conditions')),
            'night_skiing': bronze_trail.get('night_skiing', False)
        }
        
        silver['trails'].append(silver_trail)
    
    # ========================================================================
    # SECTION 5: Glades (detailed)
    # ========================================================================
    
    for bronze_glade in bronze_data.get('glades', []):
        silver_glade = {
            'name': bronze_glade.get('name'),
            'status': bronze_glade.get('status'),
            'area': bronze_glade.get('area', 'Glades'),  # Default area
            'difficulty': standardize_difficulty(bronze_glade.get('difficulty'))
        }
        
        silver['glades'].append(silver_glade)
    
    return silver


if __name__ == '__main__':
    """Test with sample Bronze data"""
    import json
    
    sample_bronze = {
        "metadata": {
            "mountain": "cannon",
            "scraped_at": "2026-01-23T04:36:04.905268",
            "scraper_version": "2.1"
        },
        "summary_metrics": {
            "mountain_report_page": {
                "open_trails": "92",
                "open_lifts": "8",
                "snowfall_new": "3\"",
                "snowfall_to_date": "88\""
            }
        },
        "weather": {
            "base": {
                "temperature_low": "23°F",
                "temperature_high": "28°F",
                "wind_direction": "W/SW",
                "wind_speed": "5-12 mph"
            },
            "summit": {
                "wind_direction": "W/SW",
                "wind_speed": "17-25 mph"
            }
        },
        "lifts": [
            {"name": "Peabody Express Quad", "status": "open", "hours": "9:00 a.m."}
        ],
        "trails": [
            {"name": "Zoomer", "status": "open", "area": "Upper Mountain",
             "difficulty": "blue", "conditions": None, "night_skiing": False}
        ],
        "glades": [
            {"name": "Kinsman Glade", "status": "open", "area": "Glades",
             "difficulty": "black", "conditions": None}
        ]
    }
    
    silver = transform_cannon(sample_bronze)
    print(json.dumps(silver, indent=2))