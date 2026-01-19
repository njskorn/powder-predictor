from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os
from pathlib import Path
import boto3
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = FastAPI(title="Powder Predictor API")

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id=os.getenv('MINIO_ROOT_USER'),
    aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD')
)

# CORS middleware (allows web/ to call api/)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/health")
def health():
    """Health check endpoint"""
    return {"status": "healthy", "service": "powder-predictor"}

@app.get("/api/conditions")
def get_conditions():
    """Get latest snow conditions - placeholder for now"""
    try:
        # get today's date
        date_str = datetime.now().strftime('%Y-%m-%d')
        s3_key = f'daily/{date_str}/conditions.json'

        # fetch snow report data from MinIO
        response = s3.get_object(Bucket='silver-snow-reports', Key=s3_key)
        raw_data = json.loads(response['Body'].read().decode('utf-8'))

        # Transform data to match front end expectations
        mountains = []
        for mountain in raw_data:
            name_map = {
                'cannon': 'Cannon Mountain',
                'bretton-woods': 'Bretton Woods',
                'cranmore': 'Cranmore'
            }

            transformed = {
                'name':name_map.get(mountain['mountain_name'], mountain['mountain_name']),
                'trails_open': f"{mountain['trails_open']}/{mountain['trails_total']}" if mountain['trails_open'] and mountain['trails_total'] else 'N/A',
                'lifts_open': f"{mountain['lifts_open']}/{mountain['lifts_total']}" if mountain['lifts_open'] and mountain['lifts_total'] else 'N/A',
                'recent_snow': f"{int(mountain['recent_snowfall_inches'])} inches" if mountain['recent_snowfall_inches'] else 'N/A',
                'season_snow': f"{int(mountain['season_snowfall_inches'])} inches" if mountain['season_snowfall_inches'] else 'N/A',
            }

            mountains.append(transformed)

        return {"mountains":mountains}
    
    except s3.exceptions.NoSuchKey:
        # If today's data doesn't exist yet, return empty
        return {
            "mountains": [],
            "message": f"No data for {date_str} yet. Run the scrapers!"
        }
    
    except Exception as e:
        return {
            "mountains": [],
            "message": f"Error: {str(e)}"
        }


# Mount static files (CSS, JS)
BASE_DIR = Path(__file__).parent.parent
WEB_DIR = BASE_DIR / "web"
app.mount("/", StaticFiles(directory=str(WEB_DIR), html=True), name="web")