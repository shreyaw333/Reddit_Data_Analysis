🔥 Reddit Data Engineering Pipeline
A comprehensive ETL pipeline to extract, transform, and analyze Reddit data using modern data engineering tools.

🎯 Project Overview
This project creates a production-ready data pipeline that:

📊 Extracts data from Reddit using the official API
🔄 Transforms raw data into structured, analytics-ready format
📈 Loads into AWS Redshift for powerful analytics
📱 Visualizes insights through AWS QuickSight dashboards

🏗️ Architecture
Reddit API → Airflow → S3 → AWS Glue → Redshift → QuickSight
     ↓         ↓        ↓       ↓         ↓         ↓
  Raw Data  Scheduler Storage Transform Warehouse Analytics
Components:

🔴 Reddit API: Data source for posts and comments
🌪️ Apache Airflow: Orchestration and scheduling
🏗️ Celery: Distributed task processing
🐘 PostgreSQL: Metadata and temporary storage
☁️ Amazon S3: Raw data lake storage
🔧 AWS Glue: ETL transformations and data catalog
🔍 Amazon Athena: SQL-based data querying
🏪 Amazon Redshift: Data warehouse for analytics
📊 AWS QuickSight: Interactive dashboards

📋 Prerequisites

AWS Account with permissions for S3, Glue, Athena, Redshift, QuickSight
Reddit API credentials (Get them here)
Docker and Docker Compose
Python 3.9+
Git

🚀 Quick Start
1. Clone the Repository
bashgit clone https://github.com/yourusername/RedditDataEngineering.git
cd RedditDataEngineering
2. Set Up Environment
bash# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
3. Configure Credentials
bash# Copy environment template
cp .env.example .env

# Edit .env with your credentials
nano .env

# Copy config template
cp config/config.conf.example config/config.conf
4. Start the Pipeline
bash# Start all services
docker-compose up -d

# Access Airflow UI
open http://localhost:8080
# Username: airflow, Password: airflow
🔧 Configuration
Reddit API Setup

Go to Reddit Apps
Create a new "script" application
Add credentials to .env file

AWS Setup

Configure AWS CLI: aws configure
Create S3 bucket for data storage
Set up Redshift cluster
Configure QuickSight account

📊 Data Pipeline Flow
1. Data Extraction

Frequency: Daily (configurable)
Sources: Specified subreddits (technology, programming, datascience, etc.)
Data Types: Posts, comments, user metadata

2. Data Storage

Raw Data: Stored in S3 with date partitioning
Format: JSON for flexibility
Retention: 90 days (configurable)

3. Data Transformation

Cleaning: Remove deleted/removed content
Enrichment: Add sentiment analysis, engagement metrics
Structuring: Convert to optimized Parquet format

4. Data Loading

Target: Amazon Redshift
Schema: Star schema optimized for analytics
Updates: Incremental loading with deduplication

📈 Analytics & Dashboards
Key Metrics Available:

🔥 Trending Posts: Real-time viral content detection
📊 Engagement Analytics: Upvotes, comments, awards over time
🎭 Sentiment Analysis: Community mood tracking
👥 User Behavior: Activity patterns and engagement
🏆 Subreddit Performance: Cross-community comparisons

Sample Queries:
sql-- Top trending posts in the last 24 hours
SELECT title, subreddit, score, num_comments, 
       (score + num_comments) as engagement_score
FROM reddit_posts 
WHERE created_at >= CURRENT_DATE - 1
ORDER BY engagement_score DESC
LIMIT 10;

-- Sentiment trends by subreddit
SELECT subreddit, 
       AVG(sentiment_score) as avg_sentiment,
       COUNT(*) as post_count
FROM reddit_posts_enriched
WHERE created_at >= CURRENT_DATE - 7
GROUP BY subreddit;
📁 Project Structure
RedditDataEngineering/
├── 📜 README.md                    # Project documentation
├── 🐳 docker-compose.yml           # Container orchestration
├── 📦 requirements.txt             # Python dependencies
├── ⚙️ config/                      # Configuration files
├── 🌊 dags/                        # Airflow DAGs
│   ├── reddit_dag.py              # Main pipeline DAG
│   └── utils/                      # Helper functions
├── 🔧 glue_jobs/                   # AWS Glue ETL scripts
├── 💾 sql/                         # Database schemas and queries
├── 🧪 tests/                       # Unit and integration tests
└── 📋 scripts/                     # Setup and utility scripts
🔍 Monitoring & Troubleshooting
Airflow UI (http://localhost:8080)

Monitor DAG runs and task status
View logs and error messages
Trigger manual runs

Flower UI (http://localhost:5555)

Monitor Celery workers
Track task distribution

Common Issues:

Reddit API Rate Limits: Automatically handled with exponential backoff
AWS Permissions: Ensure proper IAM roles are configured
Memory Issues: Increase Docker memory allocation if needed

🧪 Testing
bash# Run unit tests
pytest tests/

# Run with coverage
pytest --cov=dags tests/

# Test specific component
pytest tests/test_reddit_extractor.py
🚀 Deployment
Production Checklist:

 Set up proper AWS IAM roles
 Configure VPC and security groups
 Set up monitoring and alerting
 Implement data backup strategies
 Configure auto-scaling for peak loads

🤝 Contributing

Fork the repository
Create a feature branch: git checkout -b feature-name
Commit changes: git commit -am 'Add new feature'
Push to branch: git push origin feature-name
Submit a Pull Request

📄 License
This project is licensed under the MIT License - see the LICENSE file for details.
🙏 Acknowledgments

Reddit API for providing access to community data
Apache Airflow community for the excellent orchestration platform
AWS for comprehensive cloud services