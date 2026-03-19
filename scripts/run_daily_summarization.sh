#!/bin/bash
cd ~/projects/powder-predictor
# Load environment variables from .env file
set -a
source .env
set +a
# Activate venv
source venv/bin/activate
python scripts/summarize_all_snow_reports.py >> logs/summarization.log 2>&1