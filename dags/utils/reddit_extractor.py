"""
Reddit Data Extractor

This module handles extracting data from Reddit using the PRAW library.
It extracts posts and comments from specified subreddits and formats
the data for storage in the data pipeline.
"""

import praw
import pandas as pd
import logging
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import configparser
import os
from dataclasses import dataclass


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class RedditConfig:
    """Configuration class for Reddit API credentials"""
    client_id: str
    client_secret: str
    user_agent: str


class RedditExtractor:
    """
    Extracts data from Reddit using PRAW (Python Reddit API Wrapper)
    
    This class handles:
    - Authentication with Reddit API
    - Extracting posts from subreddits
    - Extracting comments from posts
    - Rate limiting and error handling
    - Data formatting and cleaning
    """
    
    def __init__(self, config: RedditConfig):
        """
        Initialize Reddit extractor with configuration
        
        Args:
            config (RedditConfig): Reddit API configuration
        """
        self.config = config
        self.reddit = None
        self._authenticate()
    
    def _authenticate(self) -> None:
        """Authenticate with Reddit API"""
        try:
            self.reddit = praw.Reddit(
                client_id=self.config.client_id,
                client_secret=self.config.client_secret,
                user_agent=self.config.user_agent,
                read_only=True  # We only need read access
            )
            
            # Test authentication
            logger.info(f"Successfully authenticated. Reddit instance: {self.reddit.user.me()}")
            
        except Exception as e:
            logger.error(f"Failed to authenticate with Reddit: {str(e)}")
            raise
    
    def extract_posts(self, 
                     subreddit_name: str, 
                     limit: int = 100,
                     time_filter: str = 'day',
                     sort_type: str = 'hot') -> List[Dict[str, Any]]:
        """
        Extract posts from a specific subreddit
        
        Args:
            subreddit_name (str): Name of the subreddit (without 'r/')
            limit (int): Maximum number of posts to extract
            time_filter (str): Time filter ('day', 'week', 'month', 'year', 'all')
            sort_type (str): Sort type ('hot', 'new', 'top', 'rising')
            
        Returns:
            List[Dict[str, Any]]: List of post dictionaries
        """
        posts_data = []
        
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            logger.info(f"Extracting {limit} {sort_type} posts from r/{subreddit_name}")
            
            # Get posts based on sort type
            if sort_type == 'hot':
                posts = subreddit.hot(limit=limit)
            elif sort_type == 'new':
                posts = subreddit.new(limit=limit)
            elif sort_type == 'top':
                posts = subreddit.top(time_filter=time_filter, limit=limit)
            elif sort_type == 'rising':
                posts = subreddit.rising(limit=limit)
            else:
                raise ValueError(f"Invalid sort_type: {sort_type}")
            
            for post in posts:
                post_data = self._extract_post_data(post)
                posts_data.append(post_data)
                
                # Rate limiting - be respectful to Reddit's servers
                time.sleep(0.1)
            
            logger.info(f"Successfully extracted {len(posts_data)} posts from r/{subreddit_name}")
            return posts_data
            
        except Exception as e:
            logger.error(f"Error extracting posts from r/{subreddit_name}: {str(e)}")
            raise
    
    def _extract_post_data(self, post) -> Dict[str, Any]:
        """
        Extract relevant data from a Reddit post
        
        Args:
            post: PRAW Submission object
            
        Returns:
            Dict[str, Any]: Post data dictionary
        """
        return {
            # Post identifiers
            'id': post.id,
            'fullname': post.fullname,
            'permalink': post.permalink,
            'url': post.url,
            
            # Post content
            'title': post.title,
            'selftext': post.selftext,  # Post body text
            'is_self': post.is_self,  # True if text post, False if link post
            
            # Post metadata
            'author': str(post.author) if post.author else '[deleted]',
            'subreddit': str(post.subreddit),
            'subreddit_id': post.subreddit_id,
            
            # Engagement metrics
            'score': post.score,  # Upvotes minus downvotes
            'upvote_ratio': post.upvote_ratio,
            'num_comments': post.num_comments,
            'gilded': post.gilded,  # Number of Reddit awards
            'total_awards_received': post.total_awards_received,
            
            # Timestamps
            'created_utc': datetime.fromtimestamp(post.created_utc, tz=timezone.utc),
            'extracted_at': datetime.now(timezone.utc),
            
            # Post characteristics
            'over_18': post.over_18,  # NSFW flag
            'spoiler': post.spoiler,
            'locked': post.locked,
            'stickied': post.stickied,
            'is_video': post.is_video,
            'is_original_content': post.is_original_content,
            
            # Post flair
            'link_flair_text': post.link_flair_text,
            'author_flair_text': post.author_flair_text,
            
            # Media information
            'domain': post.domain,
            'post_hint': getattr(post, 'post_hint', None),  # Type of post content
        }
    
    def extract_comments(self, 
                        post_id: str, 
                        limit: int = 100) -> List[Dict[str, Any]]:
        """
        Extract comments from a specific post
        
        Args:
            post_id (str): Reddit post ID
            limit (int): Maximum number of comments to extract
            
        Returns:
            List[Dict[str, Any]]: List of comment dictionaries
        """
        comments_data = []
        
        try:
            submission = self.reddit.submission(id=post_id)
            
            # Expand all comment trees (this can be slow for large posts)
            submission.comments.replace_more(limit=0)
            
            comment_count = 0
            for comment in submission.comments.list():
                if comment_count >= limit:
                    break
                    
                comment_data = self._extract_comment_data(comment, post_id)
                comments_data.append(comment_data)
                comment_count += 1
                
                # Rate limiting
                time.sleep(0.05)
            
            logger.info(f"Extracted {len(comments_data)} comments from post {post_id}")
            return comments_data
            
        except Exception as e:
            logger.error(f"Error extracting comments from post {post_id}: {str(e)}")
            raise
    
    def _extract_comment_data(self, comment, post_id: str) -> Dict[str, Any]:
        """
        Extract relevant data from a Reddit comment
        
        Args:
            comment: PRAW Comment object
            post_id (str): ID of the parent post
            
        Returns:
            Dict[str, Any]: Comment data dictionary
        """
        return {
            # Comment identifiers
            'id': comment.id,
            'fullname': comment.fullname,
            'permalink': comment.permalink,
            'submission_id': post_id,
            'parent_id': comment.parent_id,
            
            # Comment content
            'body': comment.body,
            'is_submitter': comment.is_submitter,  # True if comment by post author
            
            # Comment metadata
            'author': str(comment.author) if comment.author else '[deleted]',
            'subreddit': str(comment.subreddit),
            
            # Engagement metrics
            'score': comment.score,
            'gilded': comment.gilded,
            'total_awards_received': comment.total_awards_received,
            
            # Timestamps
            'created_utc': datetime.fromtimestamp(comment.created_utc, tz=timezone.utc),
            'extracted_at': datetime.now(timezone.utc),
            
            # Comment characteristics
            'stickied': comment.stickied,
            'locked': comment.locked,
            'depth': comment.depth,  # How deep in comment thread
            
            # Flair
            'author_flair_text': comment.author_flair_text,
        }
    
    def extract_subreddit_batch(self, 
                               subreddit_names: List[str],
                               posts_per_subreddit: int = 100,
                               include_comments: bool = False,
                               comments_per_post: int = 50) -> Dict[str, Any]:
        """
        Extract posts (and optionally comments) from multiple subreddits
        
        Args:
            subreddit_names (List[str]): List of subreddit names
            posts_per_subreddit (int): Number of posts per subreddit
            include_comments (bool): Whether to extract comments
            comments_per_post (int): Number of comments per post (if include_comments=True)
            
        Returns:
            Dict[str, Any]: Dictionary containing posts and comments data
        """
        all_posts = []
        all_comments = []
        
        for subreddit_name in subreddit_names:
            try:
                logger.info(f"Processing subreddit: r/{subreddit_name}")
                
                # Extract posts
                posts = self.extract_posts(
                    subreddit_name=subreddit_name,
                    limit=posts_per_subreddit
                )
                all_posts.extend(posts)
                
                # Extract comments if requested
                if include_comments:
                    for post in posts[:10]:  # Limit to first 10 posts for comments
                        comments = self.extract_comments(
                            post_id=post['id'],
                            limit=comments_per_post
                        )
                        all_comments.extend(comments)
                
                # Be respectful between subreddits
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Failed to process r/{subreddit_name}: {str(e)}")
                continue
        
        return {
            'posts': all_posts,
            'comments': all_comments,
            'extraction_timestamp': datetime.now(timezone.utc),
            'total_posts': len(all_posts),
            'total_comments': len(all_comments),
            'subreddits_processed': subreddit_names
        }
    
    def to_dataframes(self, data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """
        Convert extracted data to pandas DataFrames
        
        Args:
            data (Dict[str, Any]): Data from extract_subreddit_batch
            
        Returns:
            Dict[str, pd.DataFrame]: DataFrames for posts and comments
        """
        dataframes = {}
        
        if data['posts']:
            posts_df = pd.DataFrame(data['posts'])
            dataframes['posts'] = posts_df
            logger.info(f"Created posts DataFrame with {len(posts_df)} rows")
        
        if data['comments']:
            comments_df = pd.DataFrame(data['comments'])
            dataframes['comments'] = comments_df
            logger.info(f"Created comments DataFrame with {len(comments_df)} rows")
        
        return dataframes


def load_config_from_file(config_path: str = 'config/config.conf') -> RedditConfig:
    """
    Load Reddit configuration from config file
    
    Args:
        config_path (str): Path to configuration file
        
    Returns:
        RedditConfig: Reddit configuration object
    """
    config = configparser.ConfigParser()
    config.read(config_path)
    
    return RedditConfig(
        client_id=config.get('reddit', 'client_id'),
        client_secret=config.get('reddit', 'client_secret'),
        user_agent=config.get('reddit', 'user_agent')
    )


def load_config_from_env() -> RedditConfig:
    """
    Load Reddit configuration from environment variables
    
    Returns:
        RedditConfig: Reddit configuration object
    """
    return RedditConfig(
        client_id=os.getenv('REDDIT_CLIENT_ID'),
        client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
        user_agent=os.getenv('REDDIT_USER_AGENT')
    )


# Example usage and testing
if __name__ == "__main__":
    # Example of how to use the RedditExtractor
    
    # Load configuration (you can use either method)
    try:
        config = load_config_from_env()  # or load_config_from_file()
    except:
        # Fallback for testing
        config = RedditConfig(
            client_id="your_client_id",
            client_secret="your_client_secret",
            user_agent="TestScript/1.0"
        )
    
    # Initialize extractor
    extractor = RedditExtractor(config)
    
    # Extract data from multiple subreddits
    subreddits = ['technology', 'programming', 'datascience']
    
    data = extractor.extract_subreddit_batch(
        subreddit_names=subreddits,
        posts_per_subreddit=50,
        include_comments=True,
        comments_per_post=25
    )
    
    # Convert to DataFrames
    dataframes = extractor.to_dataframes(data)
    
    # Display results
    if 'posts' in dataframes:
        print(f"Posts DataFrame shape: {dataframes['posts'].shape}")
        print("\nSample posts:")
        print(dataframes['posts'][['title', 'subreddit', 'score', 'num_comments']].head())
    
    if 'comments' in dataframes:
        print(f"\nComments DataFrame shape: {dataframes['comments'].shape}")
        print("\nSample comments:")
        print(dataframes['comments'][['body', 'score', 'subreddit']].head())