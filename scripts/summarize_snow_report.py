"""
Snow Report Summarization Module
=================================

Reusable summarization using DistilBART for Silver layer transformations.

Usage in DAG:
    from summarize_snow_report import summarize_report
    
    summary = summarize_report(narrative_report)
"""

import re
from transformers import pipeline
import warnings

warnings.filterwarnings('ignore')
_summarizer = None

def get_summarizer():
    global _summarizer
    if _summarizer is None:
        print("Loading DistilBART summarization model...")
        _summarizer = pipeline(
            "summarization",
            model="sshleifer/distilbart-cnn-12-6",
            device=-1
        )
        print("Model loaded")
    return _summarizer

def extract_snow_conditions(text: str) -> str:
    """Extract sentences about snow conditions, filtering out events/sales"""
    sentences = re.split(r'(?<=[.!?])\s+', text)
    
    snow_keywords = [
        'snow', 'powder', 'base', 'depth', 'grooming', 'groomed',
        'conditions', 'trails', 'lifts', 'open', 'closed',
        'temperature', 'forecast', 'skiing', 'terrain', 'inches',
        'cover', 'packed', 'icy', 'fresh', 'snowmaking', 'weather',
        'corduroy'
    ]
    
    exclude_keywords = [
        'merchandise', 'shop', 'retail', 'sale', 'discount',
        'event', 'party', 'concert', 'celebration', 'dining',
        'restaurant', 'bar', 'apres', 'happy hour', 'menu', 'deal',
        'tonight','santa','easter'
    ]
    
    relevant_sentences = []
    for sentence in sentences:
        sentence_lower = sentence.lower()
        
        if any(keyword in sentence_lower for keyword in exclude_keywords):
            continue
        
        if any(keyword in sentence_lower for keyword in snow_keywords):
            relevant_sentences.append(sentence.strip())
    
    return ' '.join(relevant_sentences)

def summarize_report(text: str, max_summary_words: int = 80) -> str:
    """Summarize snow report focusing on conditions"""
    if not text or len(text.strip()) == 0:
        return ""
    
    # Filter to snow conditions only
    snow_only = extract_snow_conditions(text)
    
    if len(snow_only.split()) < 30:
        return snow_only  # Too short to summarize
    
    # Truncate to 100 words
    words = snow_only.split()
    if len(words) > 100:
        snow_only = ' '.join(words[:100])
    
    try:
        summarizer = get_summarizer()
        
        result = summarizer(
            snow_only,
            max_length=80,
            min_length=30,
            do_sample=False
        )
        
        return result[0]['summary_text']
        
    except Exception as e:
        print(f"Summarization error: {e}")
        return snow_only

if __name__ == '__main__':
    test_text = """
    Cannon received 3 inches overnight. Grooming excellent on Front Five.
    Base depth 52 inches. All lifts operating. Tonight's concert at 7pm!
    Shop our spring sale - 40% off!
    """
    
    print("Testing summarization...")
    summary = summarize_report(test_text)
    print(f"\nOriginal: {len(test_text.split())} words")
    print(f"Summary: {len(summary.split())} words")
    print(f"\n{summary}")