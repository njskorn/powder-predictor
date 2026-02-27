"""
Cranmore: Bronze → Silver Transformation
=========================================

Transforms raw Cranmore scrape data into standardized Silver layer format
Cranmore has variable snowfall reporting - use preference hierarchy
"""

from datetime import datetime
from typing import Dict, Any, Tuple, Optional
from silver_utils import (
    parse_fraction, calculate_percent, parse_lift_capacity,
    standardize_difficulty, is_groomed, parse_snowfall,
    calculate_age_minutes, count_by_difficulty, create_weather_field,
    create_summary_metric
)

def get_cranmore_snowfall(metrics: Dict[str, Any]) -> Tuple[Optional[str], str]:
    """
    Get snowfall value and unit using Cranmore's preference hierarchy
    
    Preference order:
    1. snowfall_72hr (preferred)
    2. snowfall_48hr
    3. snowfall_7day (fallback)
    
    Returns:
        (value, unit) tuple, e.g., ("5", "72hr")
    """
    # Check 72hr first (preferred)
    if 'snowfall_72hr' in metrics:
        value = parse_snowfall(metrics['snowfall_72hr'])
        if value:
            return value, '72hr'
    
    # Check 48hr
    if 'snowfall_48hr' in metrics:
        value = parse_snowfall(metrics['snowfall_48hr'])
        if value:
            return value, '48hr'
    
    # Check 7day (fallback)
    if 'snowfall_7day' in metrics:
        value = parse_snowfall(metrics['snowfall_7day'])
        if value:
            return value, '7day'
    
    return None, '7day'  # Default unit if nothing found


def transform_cranmore(bronze_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform Cranmore Bronze layer data to Silver layer
    
    Args:
        bronze_data: Raw Bronze layer JSON from scraper
        
    Returns:
        Standardized Silver layer JSON
    """
    
    processed_at = datetime.now().isoformat()
    source_scraped_at = bronze_data['metadata'].get('scraped_at', processed_at)

    # Separate glades from trails if needed (for old Bronze files)
    bronze_trails = bronze_data.get('trails', [])
    bronze_glades = bronze_data.get('glades', [])
    
    KNOWN_GLADES = ['Gibson Chutes', 'Jughandle', 'Red Line']
    
    # Extract glades from trails (handles old Bronze format)
    glades_from_trails = [
        trail for trail in bronze_trails
        if 'glade' in trail.get('name', '').lower() 
           or trail.get('name') in KNOWN_GLADES
    ]
    
    # Add to bronze_glades list
    bronze_glades.extend(glades_from_trails)
    
    # Remove glades from trails
    bronze_trails = [
        trail for trail in bronze_trails
        if 'glade' not in trail.get('name', '').lower()
           and trail.get('name') not in KNOWN_GLADES
    ]
    
    # Store back in bronze_data so rest of code can use them
    bronze_data['trails'] = bronze_trails
    bronze_data['glades'] = bronze_glades
    
    # Initialize Silver structure
    silver = {
        'mountain': 'cranmore',
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
        'narrative_report': bronze_data.get('narrative_report', ''),
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
    
    metrics = bronze_data.get('summary_metrics', {})
    
    # Lifts - filter out attractions (not actual ski lifts)
    # Attractions like tubing, coaster, swing are not relevant for skiing
    attraction_keywords = ['tubing', 'coaster', 'swing', 'eagle']  # Soaring Eagle is zipline
    
    lifts_open, lifts_total = parse_fraction(metrics.get('open_lifts'))
    if lifts_open is not None and lifts_total is not None:
        silver['summary']['lifts'] = create_summary_metric(lifts_open, lifts_total)
    else:
        # Fallback: count from lifts array (excluding attractions)
        bronze_lifts = bronze_data.get('lifts', [])
        ski_lifts = [l for l in bronze_lifts if not any(kw in l.get('name', '').lower() for kw in attraction_keywords)]
        lifts_open = sum(1 for lift in ski_lifts if lift.get('status') == 'open')
        lifts_total = len(ski_lifts)
        silver['summary']['lifts'] = create_summary_metric(lifts_open, lifts_total)
    
    # Trails (excludes glades and parks which are already separated in Bronze v2.1+)
    # Just count from the trails array directly since glades/parks are already separate
    bronze_trails = bronze_data.get('trails', [])
    trails_open = sum(1 for trail in bronze_trails if trail.get('status') == 'open')
    trails_total = len(bronze_trails)
    silver['summary']['trails'] = create_summary_metric(trails_open, trails_total)
    
    # Add difficulty breakdown
    bronze_trails = bronze_data.get('trails', [])
    silver['summary']['trails']['by_difficulty'] = count_by_difficulty(bronze_trails)
    
    # Glades (from separate array)
    bronze_glades = bronze_data.get('glades', [])
    glades_open = sum(1 for glade in bronze_glades if glade.get('status') == 'open')
    glades_total = len(bronze_glades)
    silver['summary']['glades'] = create_summary_metric(glades_open, glades_total)

    # Glades summary
    bronze_glades = bronze_data.get('glades', [])
    glades_open = sum(1 for glade in bronze_glades if glade.get('status') == 'open')
    glades_total = len(bronze_glades)
    silver['summary']['glades'] = create_summary_metric(glades_open, glades_total)

    # ADD THIS: Also add glades to difficulty breakdown
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
    
    # Temperature - usually NOT AVAILABLE at Cranmore
    silver['weather']['temperature_base'] = create_weather_field(
        value=None,
        unit='F',
        available=False
    )
    
    # Recent snowfall - use preference hierarchy
    snowfall_value, snowfall_unit = get_cranmore_snowfall(metrics)
    if snowfall_value:
        silver['weather']['snowfall_recent'] = create_weather_field(
            value=snowfall_value,
            unit=snowfall_unit
        )
    else:
        silver['weather']['snowfall_recent'] = create_weather_field(
            value=None,
            unit='7day',  # Default unit
            available=False
        )
    
    # Season total
    snowfall_season = parse_snowfall(metrics.get('season_total'))
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
    # SECTION 3: Lifts (detailed - exclude attractions)
    # ========================================================================
    
    attraction_keywords = ['tubing', 'coaster', 'swing', 'eagle']
    
    for bronze_lift in bronze_data.get('lifts', []):
        # Skip attractions
        if any(kw in bronze_lift.get('name', '').lower() for kw in attraction_keywords):
            continue
        
        capacity_info = parse_lift_capacity(bronze_lift.get('name', ''))
        
        silver_lift = {
            'name': bronze_lift.get('name'),
            'status': bronze_lift.get('status'),
            'capacity': capacity_info['capacity'],
            'type': capacity_info['type']
        }
        
        silver['lifts'].append(silver_lift)
    
    # ========================================================================
    # SECTION 4: Glades (detailed - now in separate array in Bronze v2.1+)
    # ========================================================================

    for bronze_glade in bronze_glades:
        print(bronze_glade)
        silver_glade = {
            'name': bronze_glade.get('name'),
            'status': bronze_glade.get('status'),
            'area': 'main',
            'difficulty': standardize_difficulty(bronze_glade.get('difficulty'))
        }
        
        silver['glades'].append(silver_glade)

    # ========================================================================
    # SECTION 5: Trails (detailed - glades already separated in Bronze v2.1+)
    # ========================================================================
    
    for bronze_trail in bronze_trails:
        silver_trail = {
            'name': bronze_trail.get('name'),
            'status': bronze_trail.get('status'),
            'area': 'main',  # Cranmore doesn't have multiple areas
            'difficulty': standardize_difficulty(bronze_trail.get('difficulty')),
            'groomed': is_groomed(bronze_trail.get('conditions')),
            'night_skiing': bronze_trail.get('night_skiing', False)
        }
        
        silver['trails'].append(silver_trail)
    
    return silver


if __name__ == '__main__':
    """Test with sample Bronze data"""
    import json
    
    sample_bronze = {
        "metadata": {
            "mountain": "cranmore",
            "scraped_at": "2026-01-23T04:36:06.224839",
            "source_url": "https://cranmore.com/snow-report",
            "scraper_version": "2.2",
            "last_updated": "Last Updated: Jan 22, 2026"
        },
        "summary_metrics": {
            "open_lifts": "4/7",
            "open_trails": "40/61",
            "base_depth": "13-25\"",
            "snowfall_7day": "5\"",
            "season_total": "38\""
        },
        "weather": {},
        "lifts": [
            {"name": "South Quad", "status": "open", "hours": "9:00am - 4:00pm"}
        ],
        "trails": [
            {"name": "Outta Luck", "status": "open", "area": "main",
             "night_skiing": False, "difficulty": "green", "conditions": "groomed"}
        ],
        "glades": [
            {"name": "Beech Glades", "status": "closed", "area": "main",
             "night_skiing": False, "difficulty": "black", "conditions": ""}
        ],
        "terrain_parks": [
            {"name": "Reed's Progression Park", "status": "open", "area": "main",
             "night_skiing": False, "difficulty": "park", "conditions": "groomed"}
        ]
    }
    
    silver = transform_cranmore(sample_bronze)
    print(json.dumps(silver, indent=2))