"""
Kinesis consumer for real-time transaction processing.
Reads from stream, computes features, stores for inference.
"""
import boto3
import json
from datetime import datetime

def process_record(record):
    """Extract and transform transaction data."""
    data = json.loads(record['Data'])
    
    return {
        'transaction_id': data['tx_id'],
        'account_id': data['account_id'],
        'amount': float(data['amount']),
        'timestamp': data['timestamp'],
        'merchant_category': data['mcc']
    }

def main():
    kinesis = boto3.client('kinesis')
    stream_name = 'transaction-stream'
    
    response = kinesis.describe_stream(StreamName=stream_name)
    shards = response['StreamDescription']['Shards']
    
    for shard in shards:
        shard_id = shard['ShardId']
        iterator = kinesis.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType='LATEST'
        )['ShardIterator']
        
        while True:
            records = kinesis.get_records(ShardIterator=iterator)
            
            for record in records['Records']:
                processed = process_record(record)
                # Send to feature store or inference queue
                print(f"Processed: {processed}")
            
            iterator = records['NextShardIterator']

if __name__ == "__main__":
    main()
