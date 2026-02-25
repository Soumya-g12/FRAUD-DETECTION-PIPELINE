"""
Redis client for feature caching.
Sub-millisecond lookups for hot features.
"""
import redis
import json
from typing import Dict, Optional

class FeatureCache:
    def __init__(self, host='localhost', port=6379, ttl=300):
        self.client = redis.Redis(host=host, port=port, decode_responses=True)
        self.ttl = ttl  # 5 minute default
    
    def get_features(self, account_id: str) -> Optional[Dict]:
        """Fetch features from cache."""
        key = f"features:{account_id}"
        data = self.client.get(key)
        return json.loads(data) if data else None
    
    def set_features(self, account_id: str, features: Dict):
        """Store features with TTL."""
        key = f"features:{account_id}"
        self.client.setex(key, self.ttl, json.dumps(features))
    
    def invalidate(self, account_id: str):
        """Remove stale features."""
        self.client.delete(f"features:{account_id}")
    
    def warm_cache(self, account_ids, feature_loader):
        """Pre-load features for high-priority accounts."""
        for account_id in account_ids:
            features = feature_loader(account_id)
            self.set_features(account_id, features)
