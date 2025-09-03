"""
Reddit Data Pipeline DAG - Simplified Version

This simplified DAG focuses on Reddit extraction and S3 upload
without the complex AWS Glue integration (for now).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
import os
import sys

# Add dags directory to path for imports
sys.path.append('/opt/airflow/dags')

from utils.reddit_extractor import RedditExtractor, RedditConfig
from utils.s3_utils import S3DataUploader, S3Config

# Configure logging
logger = logging.getLogger(__name__)

# DAG Configuration
DAG_ID = 'reddit_data_pipeline_simple'
SCHEDULE_INTERVAL = '0 6 * * *'  # Daily at 6 AM UTC
START_DATE = days_ago(1)
CATCHUP = False

# Pipeline Configuration
SUBREDDITS = ['technology', 'programming', 'datascience']
POSTS_PER_SUBREDDIT = 20
INCLUDE_COMMENTS = False  # Simplified - no comments for now

# Default arguments for all tasks
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': START_DATE,
    'email_on_failure': False,  # Simplified - no email alerts
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# Create the DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Simplified Reddit Data Pipeline - Extract and Store in S3',
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=CATCHUP,
    max_active_runs=1,
    tags=['reddit', 'social-media', 'etl', 'simplified'],
)

def extract_reddit_data_task(**context):
    """
    Extract data from Reddit API and upload to S3
    """
    try:
        logger.info("Starting Reddit data extraction...")
        
        # Load Reddit configuration from environment
        reddit_config = RedditConfig(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT', 'DataPipelineAnalytics/1.0 by RedditPipeline')
        )
        
        # Load S3 configuration from environment
        s3_config = S3Config(
            bucket_name=os.getenv('S3_BUCKET_NAME'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region=os.getenv('AWS_DEFAULT_REGION', 'us-east-2')
        )
        
        # Initialize extractors
        reddit_extractor = RedditExtractor(reddit_config)
        s3_uploader = S3DataUploader(s3_config)
        
        # Extract Reddit data
        logger.info(f"Extracting data from subreddits: {SUBREDDITS}")
        data = reddit_extractor.extract_subreddit_batch(
            subreddit_names=SUBREDDITS,
            posts_per_subreddit=POSTS_PER_SUBREDDIT,
            include_comments=INCLUDE_COMMENTS
        )
        
        logger.info(f"Extracted {data['total_posts']} posts and {data['total_comments']} comments")
        
        # Upload to S3
        logger.info("Uploading raw data to S3...")
        uploaded_files = s3_uploader.upload_reddit_data(
            data=data,
            file_format='json',
            data_category='raw'
        )
        
        # Create summary
        extraction_summary = {
            'total_posts': data['total_posts'],
            'total_comments': data['total_comments'],
            'subreddits_processed': data['subreddits_processed'],
            'uploaded_files': uploaded_files,
            'extraction_timestamp': data['extraction_timestamp'].isoformat(),
            's3_bucket': s3_config.bucket_name,
        }
        
        logger.info(f"Upload successful! Summary: {extraction_summary}")
        
        # Return summary for next tasks
        return extraction_summary
        
    except Exception as e:
        logger.error(f"Reddit extraction failed: {str(e)}")
        raise

def validate_data_task(**context):
    """
    Validate the extraction results
    """
    try:
        # Get task instance to pull data from previous task
        task_instance = context['task_instance']
        extraction_summary = task_instance.xcom_pull(task_ids='extract_reddit_data')
        
        if not extraction_summary:
            raise ValueError("No extraction summary found from upstream task")
        
        # Simple validation
        total_posts = extraction_summary.get('total_posts', 0)
        
        if total_posts < 5:  # Minimum threshold
            raise ValueError(f"Insufficient data extracted: {total_posts} posts (minimum: 5)")
        
        logger.info(f"Data validation passed: {total_posts} posts extracted")
        
        return {
            'validation_passed': True,
            'total_posts': total_posts,
            'validation_timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Data validation failed: {str(e)}")
        raise

def send_notification_task(**context):
    """
    Send success notification
    """
    try:
        # Get data from previous tasks
        task_instance = context['task_instance']
        extraction_summary = task_instance.xcom_pull(task_ids='extract_reddit_data')
        validation_result = task_instance.xcom_pull(task_ids='validate_data')
        
        # Create success message
        message = f"""
        🎉 Reddit Data Pipeline Completed Successfully!
        
        📊 Extraction Results:
        - Posts: {extraction_summary.get('total_posts', 0)}
        - Comments: {extraction_summary.get('total_comments', 0)}
        - Subreddits: {', '.join(extraction_summary.get('subreddits_processed', []))}
        
        ✅ Data Quality: PASSED
        ☁️ S3 Upload: SUCCESS
        
        Pipeline executed on: {context['ds']}
        """
        
        logger.info(message)
        return message
        
    except Exception as e:
        logger.error(f"Failed to send notification: {str(e)}")
        return f"Notification failed: {str(e)}"

# Define tasks
extract_task = PythonOperator(
    task_id='extract_reddit_data',
    python_callable=extract_reddit_data_task,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data_task,
    dag=dag,
)

notify_task = PythonOperator(
    task_id='send_notification',
    python_callable=send_notification_task,
    dag=dag,
)

# Define task dependencies
extract_task >> validate_task >> notify_task

# Add documentation
dag.doc_md = """
# Simplified Reddit Data Pipeline

This DAG extracts Reddit data and stores it in S3.

## What it does:
1. **Extract**: Get posts from technology, programming, datascience subreddits
2. **Validate**: Check data quality and completeness  
3. **Store**: Upload to S3 in organized structure
4. **Notify**: Log success metrics

## Schedule:
- Runs daily at 6 AM UTC
- Processes 20 posts per subreddit
- No comments extraction (simplified)

## Next Steps:
- Add AWS Glue transformation
- Add Redshift loading
- Add QuickSight dashboard
"""