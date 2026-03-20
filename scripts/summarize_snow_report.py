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
    print(f"Starting summarization...")
    print(f"Input length: {len(text.split())} words")

    if not text or len(text.strip()) == 0:
        print("Empty text, returning empty")
        return ""
    
    words = text.split()
    
    # Skip if too short to summarize
    if len(words) < 30:
        print(f"Too short ({len(words)} words), returning original")
        return text
    
    # Truncate if too long (model limit ~1024 tokens)
    max_input_words = 700
    if len(words) > max_input_words:
        print(f"Truncating from {len(words)} to {max_input_words} words")
        text = ' '.join(words[:max_input_words])
    
    try:
        print("Getting summarizer...")
        summarizer = get_summarizer()
        
        # Calculate output length based on target
        print("Running inference (this may take 30-60s)...")
        max_len = min(250, max_summary_words * 1.2)  # Allow some buffer
        min_len = min(60, max_len - 30)
        
        result = summarizer(
            text,
            max_length=int(max_len),
            min_length=int(min_len),
            do_sample=False,
            truncation=True,
            max_new_tokens=int(max_len)
        )
        
        summary = result[0]['summary_text']
        print(f"Generated summary: {len(summary.split())} words")
        return summary
        
    except Exception as e:
        print(f"Summarization error: {e}")
        print(f"Returning original text")
        return text


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