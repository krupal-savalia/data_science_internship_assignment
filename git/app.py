import logging
import feedparser
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
from celery import Celery
import spacy

# Logging Configuration
logging.basicConfig(
    filename='news_app.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Celery Configuration
app = Celery('news_processor', broker='redis://localhost:6379/0')

# Database Configuration
DATABASE_URL = "postgresql://username:password@localhost/news_db"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

# Define the NewsArticle Table
class NewsArticle(Base):
    __tablename__ = 'news_articles'
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String, nullable=False)
    summary = Column(Text, nullable=False)
    pub_date = Column(DateTime, nullable=False)
    source_url = Column(String, unique=True, nullable=False)
    category = Column(String, nullable=False)

# Create the table if it doesn't exist
Base.metadata.create_all(engine)

# Load spaCy language model
nlp = spacy.load("en_core_web_sm")

# Define the Celery task for processing an article
@app.task(bind=True, max_retries=3)
def process_article(self, article_data):
    title = article_data['title']
    summary = article_data['summary']
    pub_date = datetime.strptime(article_data['pub_date'], '%a, %d %b %Y %H:%M:%S %Z')
    source_url = article_data['source_url']
    
    logging.info(f"Processing article: {title}")
    
    try:
        # Classify the article using NLP
        category = classify_article(summary)
        
        # Add the article to the database
        new_article = NewsArticle(
            title=title,
            summary=summary,
            pub_date=pub_date,
            source_url=source_url,
            category=category
        )
        
        session.add(new_article)
        session.commit()
        logging.info(f"Article '{title}' added successfully.")
    
    except IntegrityError:
        session.rollback()
        logging.warning(f"Duplicate article detected: {title}. Skipping.")
    
    except Exception as e:
        logging.error(f"Error processing article '{title}': {e}")
        raise self.retry(exc=e, countdown=10)

# RSS Feed Parsing Function
def parse_rss_feed(feed_url):
    logging.info(f"Starting to parse RSS feed: {feed_url}")
    
    try:
        feed = feedparser.parse(feed_url)
        
        if feed.bozo:
            logging.error(f"Error parsing feed {feed_url}: {feed.bozo_exception}")
            return None
        
        logging.info(f"Successfully parsed RSS feed: {feed_url}")
        return feed.entries
    
    except Exception as e:
        logging.error(f"Failed to parse feed {feed_url}: {e}")
        return None

# Article Classification Function using spaCy
def classify_article(content):
    doc = nlp(content)
    
    if any(token.lemma_ == "terror" for token in doc):
        return "Terrorism"
    elif any(token.lemma_ == "earthquake" for token in doc):
        return "Natural Disasters"
    else:
        return "Others"

# Function to Start Processing Feeds
def process_feeds():
    rss_feeds = [
        "http://rss.cnn.com/rss/cnn_topstories.rss",
        "http://qz.com/feed",
        "http://feeds.foxnews.com/foxnews/politics",
        "http://feeds.reuters.com/reuters/businessNews",
        "http://feeds.feedburner.com/NewshourWorld",
        "https://feeds.bbci.co.uk/news/world/asia/india/rss.xml"
    ]

    for feed_url in rss_feeds:
        feed_entries = parse_rss_feed(feed_url)
        
        if feed_entries is None:
            continue

        for entry in feed_entries:
            article_data = {
                "title": entry.get('title', 'No title available'),
                "summary": entry.get('summary', 'No summary available'),
                "pub_date": entry.get('published', 'No publication date available'),
                "source_url": entry.get('link', 'No link available')
            }

            # Send the article to the Celery task queue
            process_article.delay(article_data)

if __name__ == "__main__":
    process_feeds()
