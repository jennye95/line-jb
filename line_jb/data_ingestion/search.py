from line_jb.login import get_logged_in_client
import time
import requests
from bs4 import BeautifulSoup
import subprocess
import json
from pytrends.request import TrendReq
import pandas as pd
from line_jb.data_ingestion.nyc_open_data import fetch_parks_events, insert_parks_events
import os
# import snscrape.modules.twitter as sntwitter

def fetch_posts_by_hashtag(hashtag, amount=50):
    """
    Fetch recent posts for a specific Instagram hashtag.

    :param hashtag: The hashtag to search (without #).
    :param amount: Number of posts to fetch (max ~1000).
    :return: List of media objects.
    """
    cl = get_logged_in_client()
    posts = cl.hashtag_medias_recent(hashtag, amount=amount)

    # Optional: Respect rate limits if making multiple calls
    time.sleep(random.uniform(2,5))

    return posts

def fetch_twitter_posts(query, limit=10):
    """Fetch tweets using snscrape Python library (no CLI)."""
    tweets = []
    for i, tweet in enumerate(sntwitter.TwitterSearchScraper(query).get_items()):
        if i >= limit:
            break
        tweets.append({
            'content': tweet.content,
            'date': tweet.date.isoformat(),
            'username': tweet.user.username,
            'url': tweet.url
        })
    return tweets

def fetch_trending_topics(country):
    """Scrape trending topics from trends24.in."""
    url = f"https://trends24.in/{country}/"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    trending_topics = []
    for item in soup.select('.trend-card__list a'):
        trending_topics.append(item.get_text(strip=True))
    return trending_topics

def fetch_interest_by_region(keywords, timeframe='today 1-m', geo="US"):
    """Fetch interest by region for given keywords."""
    pt = TrendReq()
    # Build payload at the COUNTRY level
    pt.build_payload(keywords, timeframe=timeframe, geo=geo)
    # Get interest by region
    df = pt.interest_by_region(resolution='CITY')

    if df is None or df.empty:
        print("No data found for these keywords in the US.")
    else:
        print(df.head())

def fetch_and_store_parks_events():
    """Helper method to fetch and store NYC Parks events."""
    conn_string = os.environ.get("DATABASE_URL")
    events = fetch_parks_events()
    insert_parks_events(events, conn_string)
    print(f"Inserted {len(events)} NYC Parks events.")

if __name__ == "__main__":
#    print("\n=== Instagrapi Hashtag Posts ===")
#    posts = fetch_posts_by_hashtag("nyc", amount=5)
#    for p in posts:
#        print(p.caption or "no caption available")

#    print("\n=== Twitter Posts via SNScrape ===")
#    tweets = fetch_twitter_posts("nyc", limit=5)
#    for t in tweets:
#        print(f"{t.get('user', {}).get('username')}: {t.get('content')}")

    print("\n=== Trends24 (US) ===")
    for trend in fetch_trending_topics("united-states")[:10]:
        print(trend)

#    print("\n=== Google Trends (US) ===")
#    keywords = ["news"]
#    results = fetch_interest_by_region(keywords)

    fetch_and_store_parks_events()
