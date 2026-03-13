"""
Snow Report Summarization Module
=================================

Reusable summarization using DistilBART for Silver layer transformations.

Usage in DAG:
    from summarize_snow_report import summarize_report
    
    summary = summarize_report(narrative_report)
"""

from transformers import pipeline
import warnings

warnings.filterwarnings('ignore')

# Global variable to cache the model (load once, use many times)
_summarizer = None


def get_summarizer():
    """
    Get or create the summarizer pipeline
    
    Loads model once and caches it for subsequent calls
    """
    global _summarizer
    
    if _summarizer is None:
        print("Loading DistilBART summarization model...")
        _summarizer = pipeline(
            "summarization",
            model="sshleifer/distilbart-cnn-12-6",
            device=-1  # Use CPU (set to 0 for GPU if available)
        )
        print("Model loaded")
    
    return _summarizer


def summarize_report(text: str, max_summary_words: int = 100) -> str:
    """
    Summarize a snow report using DistilBART
    
    Args:
        text: Full snow report text
        max_summary_words: Target max length of summary (default 100 words)
        
    Returns:
        Summarized text, or original if too short/error
    """
    if not text or len(text.strip()) == 0:
        return ""
    
    words = text.split()
    
    # Skip if too short to summarize
    if len(words) < 30:
        return text
    
    # Truncate if too long (model limit ~1024 tokens)
    max_input_words = 700
    if len(words) > max_input_words:
        text = ' '.join(words[:max_input_words])
    
    try:
        summarizer = get_summarizer()
        
        # Calculate output length based on target
        max_len = min(260, max_summary_words * 1.3)  # Allow some buffer
        min_len = min(60, max_len - 40)
        
        result = summarizer(
            text,
            max_length=int(max_len),
            min_length=int(min_len),
            do_sample=False,
            truncation=True,
            max_new_tokens=int(max_len)
        )
        
        summary = result[0]['summary_text']
        return summary
        
    except Exception as e:
        print(f"Summarization error: {e}")
        print(f" Returning original text")
        return text


if __name__ == '__main__':
    # Test
    test_text = """
    Cannon Mountain received 2 inches of new snow overnight. All trails and lifts 
    are currently open. The base depth is 48 inches at the summit. Conditions are 
    excellent with fresh powder and groomed runs available. The Front Five trails 
    are in great shape. Mittersill area opened with all expert terrain accessible.
    Temperatures are expected to stay cold through the weekend, preserving snow quality.
    Snowmaking operations continue on select trails. Parking lots are at 60% capacity.
    """
    
    print("Testing summarization...")
    summary = summarize_report(test_text)
    print(f"\nOriginal ({len(test_text.split())} words):\n{test_text}")
    print(f"\nSummary ({len(summary.split())} words):\n{summary}")