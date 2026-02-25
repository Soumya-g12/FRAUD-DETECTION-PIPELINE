"""
FastAPI service for real-time fraud scoring.
Low-latency inference with Redis caching.
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
import joblib
import json
from datetime import datetime

app = FastAPI()
model = joblib.load('fraud_model.pkl')
redis_client = redis.Redis(host='localhost', port=6379, db=0)

class TransactionRequest(BaseModel):
    transaction_id: str
    account_id: str
    amount: float
    merchant_id: str
    timestamp: datetime

def get_cached_features(account_id: str):
    """Fetch pre-computed features from Redis."""
    cached = redis_client.get(f"features:{account_id}")
    if cached:
        return json.loads(cached)
    return None

@app.post("/score")
def score_transaction(request: TransactionRequest):
    try:
        # Get real-time features
        features = get_cached_features(request.account_id)
        
        if not features:
            raise HTTPException(status_code=404, detail="Features not found")
        
        # Add transaction-specific features
        features['amount'] = request.amount
        features['hour_of_day'] = request.timestamp.hour
        
        # Predict
        risk_score = model.predict_proba([features])[0][1]
        
        return {
            "transaction_id": request.transaction_id,
            "risk_score": float(risk_score),
            "decision": "decline" if risk_score > 0.8 else "review" if risk_score > 0.5 else "approve",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health():
    return {"status": "ok", "redis_connected": redis_client.ping()}
