from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os
from pathlib import Path

app = FastAPI(title="Powder Predictor API")

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
    return {
        "mountains": [],
        "message": "Coming soon!"
    }

# Mount static files (CSS, JS)
BASE_DIR = Path(__file__).parent.parent
WEB_DIR = BASE_DIR / "web"
app.mount("/", StaticFiles(directory=str(WEB_DIR), html=True), name="web")