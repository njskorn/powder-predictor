"""
Bretton Woods: Bronze → Silver Transformation
=============================================

Transforms raw Bretton Woods scrape data into standardized Silver layer format
"""

from datetime import datetime
from typing import Dict, Any
from silver_utils import (
    parse_fraction, calculate_percent, parse_lift_capacity,
    standardize_difficulty, is_groomed, parse_snowfall,
    calculate_age_minutes, count_by_difficulty, create_weather_field,
    create_summary_metric
)

def transform_bretton_woods(bronze_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform Bretton Woods Bronze layer data to Silver layer
    
    Args:
        bronze_data: Raw Bronze layer JSON from scraper
        
    Returns:
        Standardized Silver layer JSON
    """
    
    processed_at = datetime.now().isoformat()
    source_scraped_at = bronze_data['metadata'].get('scraped_at', processed_at)
    
    # Initialize Silver structure
    silver = {
        'mountain': 'bretton-woods',
        'silver_version': '1.0',
        'processed_at': processed_at,
        'source_scraped_at': source_scraped_at,
        'data_freshness': {
            'age_minutes': calculate_age_minutes(source_scraped_at, processed_at),
            'is_stale': False  # Will calculate below
        },
        'summary': {
            'lifts': {},
            'trails': {},
            'glades': {}
        },
        'narrative_report': bronze_data.get('narrative_report', ''),
        'weather': {},
        'lifts': [],
        'trails': [],
        'glades': []
    }
    
    # Set stale flag (>2 hours)
    silver['data_freshness']['is_stale'] = silver['data_freshness']['age_minutes'] > 120
    
    # ========================================================================
    # SECTION 1: Summary Metrics
    # ========================================================================
    
    metrics = bronze_data.get('summary_metrics', {})
    
    # Lifts
    lifts_open, lifts_total = parse_fraction(metrics.get('open_lifts'))
    if lifts_open is not None and lifts_total is not None:
        silver['summary']['lifts'] = create_summary_metric(lifts_open, lifts_total)
    else:
        # Fallback: count from lifts array
        bronze_lifts = bronze_data.get('lifts', [])
        lifts_open = sum(1 for lift in bronze_lifts if lift.get('status') == 'open')
        lifts_total = len(bronze_lifts)
        silver['summary']['lifts'] = create_summary_metric(lifts_open, lifts_total)
    
    # Trails
    trails_open, trails_total = parse_fraction(metrics.get('open_trails'))
    if trails_open is not None and trails_total is not None:
        silver['summary']['trails'] = create_summary_metric(trails_open, trails_total)
        # Add difficulty breakdown
        bronze_trails = bronze_data.get('trails', [])
        silver['summary']['trails']['by_difficulty'] = count_by_difficulty(bronze_trails)
    else:
        # Fallback: count from trails array
        bronze_trails = bronze_data.get('trails', [])
        trails_open = sum(1 for trail in bronze_trails if trail.get('status') == 'open')
        trails_total = len(bronze_trails)
        silver['summary']['trails'] = create_summary_metric(trails_open, trails_total)
        silver['summary']['trails']['by_difficulty'] = count_by_difficulty(bronze_trails)
    
    # Glades
    glades_open, glades_total = parse_fraction(metrics.get('open_glades'))
    if glades_open is not None and glades_total is not None:
        silver['summary']['glades'] = create_summary_metric(glades_open, glades_total)
    else:
        # Fallback: count from glades array
        bronze_glades = bronze_data.get('glades', [])
        glades_open = sum(1 for glade in bronze_glades if glade.get('status') == 'open')
        glades_total = len(bronze_glades)
        silver['summary']['glades'] = create_summary_metric(glades_open, glades_total)

    # Add glades to difficulty breakdown
    bronze_glades = bronze_data.get('glades', [])
    glades_open = sum(1 for g in bronze_glades if g.get('status') == 'open')
    glades_total = len(bronze_glades)
    
    if 'by_difficulty' not in silver['summary']['trails']:
        silver['summary']['trails']['by_difficulty'] = {}
    
    silver['summary']['trails']['by_difficulty']['glades'] = {
        'open': glades_open,
        'total': glades_total,
        'percent': round((glades_open / glades_total * 100) if glades_total > 0 else 0)
    }
    
    # ========================================================================
    # SECTION 2: Weather
    # ========================================================================
    
    # Temperature - NOT AVAILABLE at Bretton Woods
    silver['weather']['temperature_base'] = create_weather_field(
        value=None,
        unit='F',
        available=False
    )
    
    # Recent snowfall
    snowfall_recent = parse_snowfall(metrics.get('snowfall_recent'))
    if snowfall_recent:
        silver['weather']['snowfall_recent'] = create_weather_field(
            value=snowfall_recent,
            unit='24hr',
            quality_note='assumed 24hr unit (not specified in Bronze)'
        )
    else:
        silver['weather']['snowfall_recent'] = create_weather_field(
            value=None,
            unit='24hr',
            available=False
        )
    
    # Season total
    snowfall_season = parse_snowfall(metrics.get('snowfall_season'))
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
            'area': bronze_glade.get('area'),
            'difficulty': standardize_difficulty(bronze_glade.get('difficulty'))
        }
        
        silver['glades'].append(silver_glade)
    
    return silver


if __name__ == '__main__':
    """Test with sample Bronze data"""
    import json
    
    sample_bronze = {
        "metadata": {
            "mountain": "bretton-woods",
            "scraped_at": "2026-01-23T04:35:59.636970",
            "source_url": "https://www.brettonwoods.com/snow-trail-report/",
            "scraper_version": "2.1"
        },
        "summary_metrics": {
            "open_trails": "61/63",
            "open_glades": "25/40",
            "open_lifts": "7/9",
            "total_acreage": "447.05/470.05",
            "total_miles": "33.684/36.884"
        },
        "weather": {},
        "lifts": [
            {"name": "Telegraph T-Bar", "status": "open", "hours": None},
            {"name": "Zephyr High Speed Quad", "status": "open", "hours": None}
        ],
        "trails": [
            {"name": "Almost Home", "status": "open", "area": "Mount Rosebrook", 
             "difficulty": "green", "conditions": "groomed", "night_skiing": False},
            {"name": "Bode's Run", "status": "open", "area": "Mount Rosebrook",
             "difficulty": "black", "conditions": "groomed", "night_skiing": False}
        ],
        "glades": [
            {"name": "Mom Said No", "status": "open", "area": "Mount Rosebrook",
             "difficulty": "black", "conditions": None}
        ]
    }
    
    silver = transform_bretton_woods(sample_bronze)
    print(json.dumps(silver, indent=2))