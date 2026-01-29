"""
Silver Layer Transformations - Utilities
========================================

Common utilities for transforming Bronze → Silver layer data
"""

from datetime import datetime
import re
from typing import Optional, Dict, Any, List, Tuple

def parse_fraction(text: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Parse "7/9" format into (open, total)
    Returns (None, None) if parsing fails
    """
    if not text:
        return None, None
    
    match = re.match(r'(\d+)/(\d+)', str(text))
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None

def calculate_percent(open_count: Optional[int], total_count: Optional[int]) -> Optional[int]:
    """
    Calculate percentage, handling edge cases
    Returns None if inputs are invalid
    """
    if open_count is None or total_count is None:
        return None
    
    if total_count == 0:
        return 0
    
    return round((open_count / total_count) * 100)

def parse_lift_capacity(lift_name: str) -> Dict[str, Any]:
    """
    Extract capacity and type from lift name
    
    Examples:
        "South Quad" → {"capacity": 4, "type": "quad"}
        "Bretton Woods Skyway Gondola" → {"capacity": 6, "type": "gondola"}
        "Telegraph T-Bar" → {"capacity": 1, "type": "t-bar"}
    """
    name_lower = lift_name.lower()
    
    # Define patterns
    if 'quad' in name_lower:
        return {"capacity": 4, "type": "quad"}
    elif 'triple' in name_lower:
        return {"capacity": 3, "type": "triple"}
    elif 'double' in name_lower:
        return {"capacity": 2, "type": "double"}
    elif 'gondola' in name_lower:
        return {"capacity": 8, "type": "gondola"}  # Average for gondolas
    elif 't-bar' in name_lower or 'tbar' in name_lower:
        return {"capacity": 1, "type": "t-bar"}
    elif 'carpet' in name_lower:
        return {"capacity": 0, "type": "carpet"}  # Magic carpets don't have traditional capacity
    elif 'handle tow' in name_lower or 'rope tow' in name_lower:
        return {"capacity": 0, "type": "tow"}
    else:
        return {"capacity": None, "type": "unknown"}

def standardize_difficulty(difficulty: Optional[str]) -> Optional[str]:
    """
    Standardize difficulty ratings across mountains
    """
    if not difficulty:
        return None
    
    diff_lower = difficulty.lower()
    
    # Map variations to standard values
    if diff_lower in ['green', 'beginner', 'easiest']:
        return 'green'
    elif diff_lower in ['blue', 'intermediate', 'more difficult']:
        return 'blue'
    elif diff_lower in ['black', 'black diamond', 'difficult', 'expert']:
        return 'black'
    elif diff_lower in ['double-black', 'double black', 'expert only', 'most difficult']:
        return 'double-black'
    else:
        return None

def is_groomed(conditions: Optional[str]) -> bool:
    """
    Determine if trail is groomed based on conditions field
    """
    if not conditions:
        return False
    
    conditions_lower = conditions.lower()
    return 'groom' in conditions_lower

def parse_temperature(temp_str: Optional[str]) -> Optional[int]:
    """
    Extract numeric temperature from strings like "23°F" or "23"
    """
    if not temp_str:
        return None
    
    # Extract first number found
    match = re.search(r'(\d+)', str(temp_str))
    if match:
        return int(match.group(1))
    return None

def parse_snowfall(snowfall_str: Optional[str]) -> Optional[str]:
    """
    Extract numeric snowfall from strings like "5\"" or "5 inches"
    Returns just the number as string
    """
    if not snowfall_str:
        return None
    
    # Extract number before quote or space
    match = re.match(r'(\d+)', str(snowfall_str))
    if match:
        return match.group(1)
    return None

def calculate_age_minutes(source_scraped_at: str, processed_at: str) -> int:
    """
    Calculate age in minutes between timestamps
    """
    try:
        scraped = datetime.fromisoformat(source_scraped_at.replace('Z', '+00:00'))
        processed = datetime.fromisoformat(processed_at.replace('Z', '+00:00'))
        delta = processed - scraped
        return int(delta.total_seconds() / 60)
    except:
        return 0

def count_by_difficulty(trails: List[Dict]) -> Dict[str, Dict[str, int]]:
    """
    Count trails by difficulty level
    
    Returns:
        {
            "green": {"open": 18, "total": 20, "percent": 90},
            "blue": {"open": 28, "total": 30, "percent": 93},
            ...
        }
    """
    # Initialize counters
    counts = {
        'green': {'open': 0, 'total': 0},
        'blue': {'open': 0, 'total': 0},
        'black': {'open': 0, 'total': 0},
        'double-black': {'open': 0, 'total': 0}
    }
    
    for trail in trails:
        difficulty = standardize_difficulty(trail.get('difficulty'))
        if difficulty and difficulty in counts:
            counts[difficulty]['total'] += 1
            if trail.get('status') == 'open':
                counts[difficulty]['open'] += 1
    
    # Calculate percentages
    result = {}
    for diff, data in counts.items():
        if data['total'] > 0:  # Only include difficulties that exist
            result[diff] = {
                'open': data['open'],
                'total': data['total'],
                'percent': calculate_percent(data['open'], data['total'])
            }
    
    return result

def create_weather_field(value: Any, unit: str, available: bool = True, 
                        quality_note: Optional[str] = None) -> Dict[str, Any]:
    """
    Create standardized weather field structure
    """
    field = {
        'value': value,
        'unit': unit,
        'available': available
    }
    
    if quality_note:
        field['quality_note'] = quality_note
    
    return field

def create_summary_metric(open_count: int, total_count: int) -> Dict[str, int]:
    """
    Create standardized summary metric with open/total/percent
    """
    return {
        'open': open_count,
        'total': total_count,
        'percent': calculate_percent(open_count, total_count)
    }