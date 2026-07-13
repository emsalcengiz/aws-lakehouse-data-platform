"""
Configuration module for AWS Lakehouse Data Platform
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project root
PROJECT_ROOT = Path(__file__).parent.parent

# AWS Configuration
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID", "")

# S3 Configuration
S3_BUCKET_RAW = os.getenv("S3_BUCKET_RAW", "")
S3_BUCKET_PROCESSED = os.getenv("S3_BUCKET_PROCESSED", "")
S3_BUCKET_ANALYTICS = os.getenv("S3_BUCKET_ANALYTICS", "")

# Data paths
DATA_RAW_PATH = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_PATH = PROJECT_ROOT / "data" / "processed"
DATA_SAMPLE_PATH = PROJECT_ROOT / "data" / "sample"

# Redshift Configuration
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST", "")
REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT", 5439))
REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE", "")
REDSHIFT_USER = os.getenv("REDSHIFT_USER", "")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD", "")

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

def get_config():
    """Return configuration dictionary"""
    return {
        "aws_region": AWS_REGION,
        "aws_account_id": AWS_ACCOUNT_ID,
        "s3_bucket_raw": S3_BUCKET_RAW,
        "s3_bucket_processed": S3_BUCKET_PROCESSED,
        "s3_bucket_analytics": S3_BUCKET_ANALYTICS,
        "data_raw_path": str(DATA_RAW_PATH),
        "data_processed_path": str(DATA_PROCESSED_PATH),
        "redshift_host": REDSHIFT_HOST,
        "redshift_database": REDSHIFT_DATABASE,
    }
