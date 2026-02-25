# Real-Time Fraud Detection

Streaming ML pipeline for transaction scoring. Processes high-volume trading data with low-latency requirements.

## Flow
Kinesis Stream → Spark Feature Engineering → Redis Cache → FastAPI Inference → Response

## Key optimizations

- **Redis caching:** Hot features pre-computed, sub-millisecond lookup
- **Spark tuning:** Broadcast joins, partition pruning, caching strategies
- **Model versioning:** MLflow tracking, easy rollback on bad deployments

## Trade-offs made

- Chose Redis over DynamoDB for latency, accepted operational complexity
- Batch feature computation vs. real-time — found middle ground with streaming windows
- SageMaker vs. self-hosted — went self-hosted for cost control at scale

## Run

```bash
# Start local stack
docker-compose up redis api

# Simulate traffic
python scripts/load_test.py --rps 1000

Project structure
streaming/
├── kinesis_consumer.py      # Stream ingestion
└── feature_engineering.py   # Spark processing

serving/
├── api/
│   └── main.py              # FastAPI inference
└── caching/
    └── redis_client.py      # Feature cache

infrastructure/
└── databricks/
    └── spark_jobs.py        # Batch processing

notebooks/
└── feature_importance.ipynb # Model analysis

---
