"""
MinIO Configuration
Connection settings for accessing Silver layer data
"""

import os

# MinIO Connection Settings
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# S3 Settings
SILVER_BUCKET = "silver-snow-reports"
USE_SSL = False  # Set True if using HTTPS

# Available Mountains
MOUNTAINS = ["bretton-woods", "cannon", "cranmore"]