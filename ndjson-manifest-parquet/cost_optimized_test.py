"""
Cost-Optimized Pipeline Test Script
====================================

Generates a controlled batch of test files to minimize AWS costs.
Run this ONCE to verify the pipeline works end-to-end.

Estimated cost: $1-2 total (mostly Glue)

Usage:
    python cost_optimized_test.py

Author: Data Engineering Team
"""

import json
import os
import boto3
import random
import string
from datetime import datetime
import uuid
import time

# Configuration - EDIT THESE
INPUT_BUCKET = "ndjson-input-sqs-804450520964-dev"
INPUT_PREFIX = "pipeline/input"

# Test parameters - Keep these small to minimize cost!
FILE_COUNT = 12          # Slightly more than max_files_per_manifest (10) to trigger one manifest
FILE_SIZE_KB = 100       # Small files for testing (100 KB each)
DATE_PREFIX = datetime.utcnow().strftime('%Y-%m-%d')

# AWS clients
s3_client = boto3.client('s3')
sfn_client = boto3.client('stepfunctions')
dynamodb = boto3.resource('dynamodb')


def generate_test_record(sequence: int) -> dict:
    """Generate a single test record."""
    timestamp = datetime.utcnow()
    return {
        'id': f'test_{timestamp.strftime("%Y%m%d%H%M%S")}_{sequence:06d}',
        'timestamp': timestamp.isoformat() + 'Z',
        'event_type': random.choice(['page_view', 'click', 'purchase']),
        'user_id': f'user_{random.randint(1000, 9999)}',
        'session_id': ''.join(random.choices(string.ascii_letters, k=32)),
        'country': random.choice(['US', 'GB', 'DE', 'FR']),
        'revenue': round(random.uniform(0, 100), 2),
        'padding': ''.join(random.choices(string.ascii_letters, k=200))  # ~200 bytes padding
    }


def generate_ndjson_content(target_size_kb: int) -> str:
    """Generate NDJSON content to approximately target size."""
    lines = []
    current_size = 0
    target_bytes = target_size_kb * 1024
    sequence = 0

    while current_size < target_bytes:
        record = generate_test_record(sequence)
        line = json.dumps(record) + '\n'
        lines.append(line)
        current_size += len(line.encode('utf-8'))
        sequence += 1

    return ''.join(lines)


def upload_test_file(sequence: int) -> dict:
    """Generate and upload a single test file."""
    timestamp = datetime.utcnow().strftime('%H%M%S')
    filename = f"{DATE_PREFIX}-costtest{sequence:04d}-{timestamp}-{uuid.uuid1()}.ndjson"
    key = f"{INPUT_PREFIX}/{filename}"

    content = generate_ndjson_content(FILE_SIZE_KB)
    content_bytes = content.encode('utf-8')

    s3_client.put_object(
        Bucket=INPUT_BUCKET,
        Key=key,
        Body=content_bytes,
        ContentType='application/x-ndjson'
    )

    return {
        'key': key,
        'size_kb': len(content_bytes) / 1024,
        's3_uri': f's3://{INPUT_BUCKET}/{key}'
    }


def run_cost_optimized_test():
    """Run a single controlled test batch."""
    print("=" * 60)
    print("COST-OPTIMIZED PIPELINE TEST")
    print("=" * 60)
    print(f"Files to generate: {FILE_COUNT}")
    print(f"File size: ~{FILE_SIZE_KB} KB each")
    print(f"Date prefix: {DATE_PREFIX}")
    print(f"Target bucket: {INPUT_BUCKET}/{INPUT_PREFIX}")
    print()
    print("Estimated cost: ~$1-2 (Glue job is the main cost)")
    print("=" * 60)

    # Confirm before proceeding
    response = input("\nProceed with test? (yes/no): ")
    if response.lower() != 'yes':
        print("Test cancelled.")
        return

    print(f"\nUploading {FILE_COUNT} test files...")
    uploaded = []

    for i in range(FILE_COUNT):
        try:
            file_info = upload_test_file(i)
            uploaded.append(file_info)
            print(f"  [{i+1}/{FILE_COUNT}] Uploaded: {file_info['key']} ({file_info['size_kb']:.1f} KB)")
        except Exception as e:
            print(f"  [{i+1}/{FILE_COUNT}] ERROR: {e}")

    total_size_kb = sum(f['size_kb'] for f in uploaded)

    print()
    print("=" * 60)
    print("UPLOAD COMPLETE")
    print("=" * 60)
    print(f"Files uploaded: {len(uploaded)}/{FILE_COUNT}")
    print(f"Total size: {total_size_kb:.1f} KB ({total_size_kb/1024:.2f} MB)")
    print()
    print("NEXT STEPS:")
    print("1. Wait 1-2 minutes for Lambda to process files")
    print("2. Check CloudWatch Logs for Lambda execution")
    print("3. Check Step Functions console for workflow execution")
    print("4. Glue job will take 2-5 minutes to complete")
    print("5. Check output bucket for Parquet files")
    print()
    print("MONITORING COMMANDS:")
    print(f"  aws logs tail /aws/lambda/ndjson-parquet-manifest-builder-dev --follow")
    print(f"  aws stepfunctions list-executions --state-machine-arn <your-arn> --max-results 5")


def check_pipeline_status():
    """Check the current pipeline status."""
    print("\nChecking pipeline status...")

    # Check DynamoDB for recent file entries
    table = dynamodb.Table('ndjson-parquet-sqs-file-tracking-dev')

    response = table.query(
        KeyConditionExpression='date_prefix = :dp',
        ExpressionAttributeValues={':dp': DATE_PREFIX},
        Limit=20
    )

    items = response.get('Items', [])
    print(f"\nDynamoDB entries for {DATE_PREFIX}: {len(items)}")

    status_counts = {}
    for item in items:
        status = item.get('status', 'unknown')
        status_counts[status] = status_counts.get(status, 0) + 1

    print("Status breakdown:")
    for status, count in status_counts.items():
        print(f"  {status}: {count}")


if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'status':
        check_pipeline_status()
    else:
        run_cost_optimized_test()
