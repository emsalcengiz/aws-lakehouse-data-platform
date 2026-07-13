"""
Utility functions for AWS Lakehouse Data Platform
"""
import logging
import pandas as pd
import boto3
from pathlib import Path
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class S3Manager:
    """Manage S3 operations"""
    
    def __init__(self, region_name: str = "us-east-1"):
        self.s3_client = boto3.client("s3", region_name=region_name)
        self.s3_resource = boto3.resource("s3", region_name=region_name)
    
    def upload_file(self, file_path: str, bucket: str, key: str) -> bool:
        """Upload file to S3"""
        try:
            self.s3_client.upload_file(file_path, bucket, key)
            logger.info(f"Uploaded {file_path} to s3://{bucket}/{key}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload file: {str(e)}")
            return False
    
    def download_file(self, bucket: str, key: str, file_path: str) -> bool:
        """Download file from S3"""
        try:
            self.s3_client.download_file(bucket, key, file_path)
            logger.info(f"Downloaded s3://{bucket}/{key} to {file_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to download file: {str(e)}")
            return False
    
    def list_files(self, bucket: str, prefix: str = "") -> list:
        """List files in S3 bucket"""
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
            files = []
            for page in pages:
                if "Contents" in page:
                    files.extend([obj["Key"] for obj in page["Contents"]])
            return files
        except Exception as e:
            logger.error(f"Failed to list files: {str(e)}")
            return []


class DataProcessor:
    """Process and transform data"""
    
    @staticmethod
    def read_csv(file_path: str) -> Optional[pd.DataFrame]:
        """Read CSV file"""
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Read {len(df)} rows from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Failed to read CSV: {str(e)}")
            return None
    
    @staticmethod
    def read_parquet(file_path: str) -> Optional[pd.DataFrame]:
        """Read Parquet file"""
        try:
            df = pd.read_parquet(file_path)
            logger.info(f"Read {len(df)} rows from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Failed to read Parquet: {str(e)}")
            return None
    
    @staticmethod
    def write_parquet(df: pd.DataFrame, file_path: str, compression: str = "snappy") -> bool:
        """Write DataFrame to Parquet"""
        try:
            df.to_parquet(file_path, compression=compression, index=False)
            logger.info(f"Wrote {len(df)} rows to {file_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to write Parquet: {str(e)}")
            return False
    
    @staticmethod
    def get_data_profile(df: pd.DataFrame) -> Dict[str, Any]:
        """Get data profile/statistics"""
        return {
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": df.columns.tolist(),
            "data_types": df.dtypes.to_dict(),
            "missing_values": df.isnull().sum().to_dict(),
        }


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Setup logging configuration"""
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger(__name__)
