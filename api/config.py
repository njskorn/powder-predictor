"""
MinIO Configuration
Connection settings for accessing Silver layer data
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
# This loads the same .env that docker-compose uses
load_dotenv()

# MinIO Connection Settings
# These match the docker-compose environment variables
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

# S3 Settings
SILVER_BUCKET = "silver-snow-reports"
USE_SSL = False  # Set True if using HTTPS

# Available Mountains
MOUNTAINS = ["bretton-woods", "cannon", "cranmore"]