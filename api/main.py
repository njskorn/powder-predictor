"""
Powder Predictor API
====================

FastAPI backend serving Silver layer snow reports from MinIO
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
import logging
from typing import Dict, List

from .minio_client import get_current_report, list_mountains

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI(
    title="Powder Predictor API",
    description="Real-time ski mountain conditions for NH mountains",
    version="1.0.0"
)

# CORS middleware - allows frontend to call API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """API health check"""
    return {
        "status": "healthy",
        "service": "Powder Predictor API",
        "version": "1.0.0"
    }


@app.get("/api/mountains")
async def get_all_mountains():
    """
    Get current conditions for all mountains
    
    Returns aggregated data from Bretton Woods, Cannon, and Cranmore
    """
    mountains = ["bretton-woods", "cannon", "cranmore"]
    results = {}
    
    for mountain in mountains:
        try:
            report = get_current_report(mountain)
            results[mountain] = report
            logger.info(f"Successfully retrieved {mountain} report")
        except Exception as e:
            logger.error(f"Error retrieving {mountain}: {str(e)}")
            results[mountain] = {"error": str(e)}
    
    return results


@app.get("/api/mountains/{mountain}")
async def get_mountain(mountain: str):
    """
    Get current conditions for a specific mountain
    
    Args:
        mountain: Mountain name (bretton-woods, cannon, or cranmore)
    
    Returns:
        Current Silver layer report for the mountain
    """
    try:
        report = get_current_report(mountain)
        logger.info(f"Retrieved {mountain} report: {report.get('data_freshness', {})}")
        return report
    except FileNotFoundError:
        raise HTTPException(
            status_code=404, 
            detail=f"No data found for mountain: {mountain}"
        )
    except Exception as e:
        logger.error(f"Error retrieving {mountain}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving data: {str(e)}"
        )


@app.get("/api/summary")
async def get_summary():
    """
    Get high-level summary of all mountains
    
    Returns quick stats: % open trails, temp, freshness
    """
    mountains = ["bretton-woods", "cannon", "cranmore"]
    summaries = []
    
    for mountain in mountains:
        try:
            report = get_current_report(mountain)
            summary = {
                "mountain": mountain,
                "display_name": mountain.replace("-", " ").title(),
                "trails_open": report.get("summary", {}).get("trails", {}).get("percent", 0),
                "lifts_open": report.get("summary", {}).get("lifts", {}).get("percent", 0),
                "temperature": report.get("weather", {}).get("temperature_base", {}).get("value"),
                "data_age_minutes": report.get("data_freshness", {}).get("age_minutes", 0),
                "is_stale": report.get("data_freshness", {}).get("is_stale", True)
            }
            summaries.append(summary)
        except Exception as e:
            logger.error(f"Error getting summary for {mountain}: {str(e)}")
            summaries.append({
                "mountain": mountain,
                "error": str(e)
            })
    
    return {"mountains": summaries}


# Mount static frontend files (HTML, CSS, JS)
# This serves the dashboard UI
app.mount("/", StaticFiles(directory="../frontend", html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)