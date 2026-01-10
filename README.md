# Powder Predictor

Scrapes daily snow reports from New Hampshire ski mountains, stores data in a medallion architecture, and predicts optimal skiing conditions.

## Mountains Tracked
- Cranmore Mountain Resort
- Bretton Woods Mountain Resort  
- Cannon Mountain

### Stack
- **Orchestration**: Apache Airflow
- **Storage**: MinIO (S3-compatible)
- **Data Warehouse**: DuckDB
- **Format**: Delta Lake (Parquet)
- **Container**: Docker + Docker Compose
- **Language**: Python 3.11

## Getting Started
```bash
# Start the stack
docker-compose up -d

# View Airflow UI
open http://localhost:8080

# View MinIO UI  
open http://localhost:9000
```

## Project Status
🚧 In Development

## License
Personal project - not for commercial use