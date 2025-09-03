"""
Quick test script for Reddit Extractor
Run this to verify your Reddit API credentials work
"""

import os
import sys
sys.path.append('dags')

from utils.reddit_extractor import RedditExtractor, RedditConfig

def test_reddit_connection():
    """Test Reddit API connection with minimal data extraction"""
    
    print("🔍 Testing Reddit Data Extractor...")
    
    # Configure Reddit credentials
    config = RedditConfig(
        client_id='HoQMSzjrngUFq4eOYnlaGA',
        client_secret='IfM-5_8IovCtTvqmscHX17p9sp_ElA',
        user_agent='DataPipelineAnalytics/1.0 by InternationalCry6457'
    )
    
    try:
        # Initialize extractor
        print("🔐 Authenticating with Reddit...")
        extractor = RedditExtractor(config)
        print("✅ Authentication successful!")
        
        # Test with a small extraction
        print("\n📊 Testing data extraction from r/technology...")
        posts = extractor.extract_posts(
            subreddit_name='technology',
            limit=5,  # Just 5 posts for testing
            sort_type='hot'
        )
        
        print(f"✅ Successfully extracted {len(posts)} posts!")
        
        # Display sample data
        print("\n🔥 Sample post data:")
        for i, post in enumerate(posts[:2]):
            print(f"\nPost {i+1}:")
            print(f"  Title: {post['title'][:60]}...")
            print(f"  Score: {post['score']}")
            print(f"  Comments: {post['num_comments']}")
            print(f"  Author: {post['author']}")
        
        print("\n🎉 Reddit extractor is working perfectly!")
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print("\n🔧 Make sure you have:")
        print("  1. Created Reddit API credentials")
        print("  2. Set environment variables or updated config file")
        print("  3. Installed required packages: pip install praw pandas")
        return False

if __name__ == "__main__":
    test_reddit_connection()