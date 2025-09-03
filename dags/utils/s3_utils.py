"""
S3 Storage Utility for Reddit Data Pipeline

This module handles uploading Reddit data to AWS S3 with proper organization,
partitioning, and format handling for optimal analytics performance.
"""

import boto3
import pandas as pd
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
import io
from botocore.exceptions import ClientError, NoCredentialsError
import pyarrow as pa
import pyarrow.parquet as pq

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class S3Config:
    """Configuration class for S3 settings"""
    bucket_name: str
    aws_access_key_id: str
    aws_secret_access_key: str
    region: str = 'us-east-1'
    raw_data_prefix: str = 'raw-data'
    processed_data_prefix: str = 'processed-data'
    backup_prefix: str = 'backup'


class S3DataUploader:
    """
    Handles uploading Reddit data to AWS S3 with proper organization and formatting
    
    Features:
    - Date-based partitioning for efficient querying
    - Multiple file formats (JSON, Parquet, CSV)
    - Proper folder structure for analytics
    - Error handling and retry logic
    - Metadata tracking
    """
    
    def __init__(self, config: S3Config):
        """
        Initialize S3 uploader with configuration
        
        Args:
            config (S3Config): S3 configuration object
        """
        self.config = config
        self.s3_client = None
        self._authenticate()
    
    def _authenticate(self) -> None:
        """Authenticate with AWS S3"""
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.config.aws_access_key_id,
                aws_secret_access_key=self.config.aws_secret_access_key,
                region_name=self.config.region
            )
            
            # Test connection
            self.s3_client.head_bucket(Bucket=self.config.bucket_name)
            logger.info(f"Successfully connected to S3 bucket: {self.config.bucket_name}")
            
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            raise
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.error(f"Bucket {self.config.bucket_name} does not exist")
            else:
                logger.error(f"Failed to connect to S3: {str(e)}")
            raise
    
    def create_s3_key(self, 
                     data_type: str, 
                     subreddit: str, 
                     date: datetime, 
                     file_format: str = 'json',
                     data_category: str = 'raw') -> str:
        """
        Create a structured S3 key for organized data storage
        
        Args:
            data_type (str): Type of data ('posts', 'comments', 'metadata')
            subreddit (str): Subreddit name
            date (datetime): Date of extraction
            file_format (str): File format ('json', 'parquet', 'csv')
            data_category (str): Data category ('raw', 'processed', 'backup')
            
        Returns:
            str: S3 key path
        """
        # Choose prefix based on data category
        if data_category == 'raw':
            prefix = self.config.raw_data_prefix
        elif data_category == 'processed':
            prefix = self.config.processed_data_prefix
        elif data_category == 'backup':
            prefix = self.config.backup_prefix
        else:
            prefix = 'data'
        
        # Create date-based partitioning for efficient querying
        year = date.strftime('%Y')
        month = date.strftime('%m')
        day = date.strftime('%d')
        hour = date.strftime('%H')
        
        # Create structured path
        s3_key = f"{prefix}/{data_type}/subreddit={subreddit}/year={year}/month={month}/day={day}/hour={hour}/{data_type}_{subreddit}_{date.strftime('%Y%m%d_%H%M%S')}.{file_format}"
        
        return s3_key
    
    def upload_reddit_data(self, 
                          data: Dict[str, Any], 
                          file_format: str = 'json',
                          data_category: str = 'raw') -> Dict[str, List[str]]:
        """
        Upload Reddit data to S3 with proper organization
        
        Args:
            data (Dict[str, Any]): Data from reddit_extractor.extract_subreddit_batch()
            file_format (str): Output format ('json', 'parquet', 'csv')
            data_category (str): Data category ('raw', 'processed', 'backup')
            
        Returns:
            Dict[str, List[str]]: Dictionary of uploaded file paths
        """
        uploaded_files = {
            'posts_files': [],
            'comments_files': [],
            'metadata_files': []
        }
        
        extraction_time = data.get('extraction_timestamp', datetime.now(timezone.utc))
        
        try:
            # Upload posts data
            if data.get('posts'):
                posts_files = self._upload_posts_data(
                    posts=data['posts'],
                    extraction_time=extraction_time,
                    file_format=file_format,
                    data_category=data_category
                )
                uploaded_files['posts_files'] = posts_files
            
            # Upload comments data
            if data.get('comments'):
                comments_files = self._upload_comments_data(
                    comments=data['comments'],
                    extraction_time=extraction_time,
                    file_format=file_format,
                    data_category=data_category
                )
                uploaded_files['comments_files'] = comments_files
            
            # Upload metadata
            metadata_file = self._upload_metadata(
                data=data,
                extraction_time=extraction_time,
                data_category=data_category
            )
            uploaded_files['metadata_files'] = [metadata_file]
            
            logger.info(f"Successfully uploaded all Reddit data to S3")
            return uploaded_files
            
        except Exception as e:
            logger.error(f"Failed to upload Reddit data: {str(e)}")
            raise
    
    def _upload_posts_data(self, 
                          posts: List[Dict[str, Any]], 
                          extraction_time: datetime,
                          file_format: str,
                          data_category: str) -> List[str]:
        """Upload posts data grouped by subreddit"""
        uploaded_files = []
        
        # Group posts by subreddit for efficient storage
        posts_by_subreddit = {}
        for post in posts:
            subreddit = post['subreddit']
            if subreddit not in posts_by_subreddit:
                posts_by_subreddit[subreddit] = []
            posts_by_subreddit[subreddit].append(post)
        
        # Upload each subreddit's posts separately
        for subreddit, subreddit_posts in posts_by_subreddit.items():
            s3_key = self.create_s3_key(
                data_type='posts',
                subreddit=subreddit,
                date=extraction_time,
                file_format=file_format,
                data_category=data_category
            )
            
            # Upload based on format
            if file_format == 'json':
                self._upload_json(subreddit_posts, s3_key)
            elif file_format == 'parquet':
                df = pd.DataFrame(subreddit_posts)
                self._upload_parquet(df, s3_key)
            elif file_format == 'csv':
                df = pd.DataFrame(subreddit_posts)
                self._upload_csv(df, s3_key)
            
            uploaded_files.append(s3_key)
            logger.info(f"Uploaded {len(subreddit_posts)} posts from r/{subreddit}")
        
        return uploaded_files
    
    def _upload_comments_data(self, 
                             comments: List[Dict[str, Any]], 
                             extraction_time: datetime,
                             file_format: str,
                             data_category: str) -> List[str]:
        """Upload comments data grouped by subreddit"""
        uploaded_files = []
        
        # Group comments by subreddit
        comments_by_subreddit = {}
        for comment in comments:
            subreddit = comment['subreddit']
            if subreddit not in comments_by_subreddit:
                comments_by_subreddit[subreddit] = []
            comments_by_subreddit[subreddit].append(comment)
        
        # Upload each subreddit's comments separately
        for subreddit, subreddit_comments in comments_by_subreddit.items():
            s3_key = self.create_s3_key(
                data_type='comments',
                subreddit=subreddit,
                date=extraction_time,
                file_format=file_format,
                data_category=data_category
            )
            
            # Upload based on format
            if file_format == 'json':
                self._upload_json(subreddit_comments, s3_key)
            elif file_format == 'parquet':
                df = pd.DataFrame(subreddit_comments)
                self._upload_parquet(df, s3_key)
            elif file_format == 'csv':
                df = pd.DataFrame(subreddit_comments)
                self._upload_csv(df, s3_key)
            
            uploaded_files.append(s3_key)
            logger.info(f"Uploaded {len(subreddit_comments)} comments from r/{subreddit}")
        
        return uploaded_files
    
    def _upload_metadata(self, 
                        data: Dict[str, Any], 
                        extraction_time: datetime,
                        data_category: str) -> str:
        """Upload extraction metadata"""
        metadata = {
            'extraction_timestamp': extraction_time.isoformat(),
            'total_posts': data.get('total_posts', 0),
            'total_comments': data.get('total_comments', 0),
            'subreddits_processed': data.get('subreddits_processed', []),
            'extraction_config': {
                'posts_per_subreddit': data.get('posts_per_subreddit'),
                'include_comments': data.get('include_comments'),
                'comments_per_post': data.get('comments_per_post')
            },
            'pipeline_version': '1.0',
            'data_schema_version': '1.0'
        }
        
        # Create metadata key
        s3_key = f"{self.config.raw_data_prefix}/metadata/extraction_metadata_{extraction_time.strftime('%Y%m%d_%H%M%S')}.json"
        
        self._upload_json(metadata, s3_key)
        logger.info(f"Uploaded extraction metadata")
        
        return s3_key
    
    def _upload_json(self, data: Union[List, Dict], s3_key: str) -> None:
        """Upload data as JSON format"""
        json_buffer = io.StringIO()
        json.dump(data, json_buffer, indent=2, default=str)
        
        self.s3_client.put_object(
            Bucket=self.config.bucket_name,
            Key=s3_key,
            Body=json_buffer.getvalue(),
            ContentType='application/json'
        )
    
    def _upload_parquet(self, df: pd.DataFrame, s3_key: str) -> None:
        """Upload data as Parquet format (optimized for analytics)"""
        parquet_buffer = io.BytesIO()
        
        # Convert datetime columns for Parquet compatibility
        for col in df.columns:
            if df[col].dtype == 'object':
                # Try to convert datetime strings
                try:
                    df[col] = pd.to_datetime(df[col], errors='ignore')
                except:
                    pass
        
        # Write to Parquet
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_buffer)
        
        self.s3_client.put_object(
            Bucket=self.config.bucket_name,
            Key=s3_key,
            Body=parquet_buffer.getvalue(),
            ContentType='application/octet-stream'
        )
    
    def _upload_csv(self, df: pd.DataFrame, s3_key: str) -> None:
        """Upload data as CSV format"""
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        
        self.s3_client.put_object(
            Bucket=self.config.bucket_name,
            Key=s3_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
    
    def list_files(self, prefix: str = '', limit: int = 100) -> List[Dict[str, Any]]:
        """
        List files in S3 bucket with optional prefix filter
        
        Args:
            prefix (str): S3 key prefix to filter by
            limit (int): Maximum number of files to return
            
        Returns:
            List[Dict[str, Any]]: List of file information
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.config.bucket_name,
                Prefix=prefix,
                MaxKeys=limit
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'storage_class': obj.get('StorageClass', 'STANDARD')
                    })
            
            logger.info(f"Found {len(files)} files with prefix '{prefix}'")
            return files
            
        except Exception as e:
            logger.error(f"Failed to list S3 files: {str(e)}")
            raise
    
    def download_file(self, s3_key: str, local_path: Optional[str] = None) -> Union[str, bytes]:
        """
        Download a file from S3
        
        Args:
            s3_key (str): S3 key of the file to download
            local_path (str, optional): Local path to save file. If None, returns content
            
        Returns:
            Union[str, bytes]: File content if local_path is None, otherwise local file path
        """
        try:
            if local_path:
                self.s3_client.download_file(
                    Bucket=self.config.bucket_name,
                    Key=s3_key,
                    Filename=local_path
                )
                logger.info(f"Downloaded {s3_key} to {local_path}")
                return local_path
            else:
                response = self.s3_client.get_object(
                    Bucket=self.config.bucket_name,
                    Key=s3_key
                )
                content = response['Body'].read()
                logger.info(f"Retrieved content from {s3_key}")
                return content
                
        except Exception as e:
            logger.error(f"Failed to download {s3_key}: {str(e)}")
            raise
    
    def create_bucket_if_not_exists(self) -> bool:
        """
        Create S3 bucket if it doesn't exist
        
        Returns:
            bool: True if bucket was created or already exists
        """
        try:
            # Check if bucket exists
            self.s3_client.head_bucket(Bucket=self.config.bucket_name)
            logger.info(f"Bucket {self.config.bucket_name} already exists")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                # Bucket doesn't exist, create it
                try:
                    if self.config.region == 'us-east-1':
                        # us-east-1 doesn't need LocationConstraint
                        self.s3_client.create_bucket(Bucket=self.config.bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.config.bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': self.config.region}
                        )
                    
                    logger.info(f"Created bucket {self.config.bucket_name}")
                    return True
                    
                except Exception as create_error:
                    logger.error(f"Failed to create bucket: {str(create_error)}")
                    return False
            else:
                logger.error(f"Error checking bucket: {str(e)}")
                return False


def load_s3_config_from_file(config_path: str = 'config/config.conf') -> S3Config:
    """
    Load S3 configuration from config file
    
    Args:
        config_path (str): Path to configuration file
        
    Returns:
        S3Config: S3 configuration object
    """
    import configparser
    config = configparser.ConfigParser()
    config.read(config_path)
    
    return S3Config(
        bucket_name=config.get('s3', 'bucket_name'),
        aws_access_key_id=config.get('aws', 'access_key_id'),
        aws_secret_access_key=config.get('aws', 'secret_access_key'),
        region=config.get('aws', 'region'),
        raw_data_prefix=config.get('s3', 'raw_data_prefix'),
        processed_data_prefix=config.get('s3', 'processed_data_prefix')
    )


def load_s3_config_from_env() -> S3Config:
    """
    Load S3 configuration from environment variables
    
    Returns:
        S3Config: S3 configuration object
    """
    return S3Config(
        bucket_name=os.getenv('S3_BUCKET_NAME'),
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
        raw_data_prefix=os.getenv('S3_RAW_DATA_PREFIX', 'raw-data'),
        processed_data_prefix=os.getenv('S3_PROCESSED_DATA_PREFIX', 'processed-data')
    )


# Example usage and testing
if __name__ == "__main__":
    # Example of how to use the S3DataUploader
    
    # Load configuration
    try:
        s3_config = load_s3_config_from_env()
    except:
        # Fallback for testing
        s3_config = S3Config(
            bucket_name="your-reddit-pipeline-bucket",
            aws_access_key_id="your_aws_access_key",
            aws_secret_access_key="your_aws_secret_key",
            region="us-east-1"
        )
    
    # Initialize uploader
    uploader = S3DataUploader(s3_config)
    
    # Example data structure (from reddit_extractor)
    sample_data = {
        'posts': [
            {
                'id': 'abc123',
                'title': 'Sample Post',
                'subreddit': 'technology',
                'score': 100,
                'created_utc': datetime.now(timezone.utc)
            }
        ],
        'comments': [],
        'extraction_timestamp': datetime.now(timezone.utc),
        'total_posts': 1,
        'total_comments': 0,
        'subreddits_processed': ['technology']
    }
    
    # Upload data
    try:
        uploaded_files = uploader.upload_reddit_data(
            data=sample_data,
            file_format='json',
            data_category='raw'
        )
        
        print("Upload successful!")
        print(f"Uploaded files: {uploaded_files}")
        
        # List files
        files = uploader.list_files(prefix='raw-data/posts')
        print(f"Files in bucket: {len(files)}")
        
    except Exception as e:
        print(f"Upload failed: {str(e)}")