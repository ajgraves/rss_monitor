#!/usr/bin/env python3
import os
import hashlib
import feedparser
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import sqlite3
import time
from datetime import datetime, timedelta
import requests
from readability import Document
from urllib.parse import urlparse

# Configuration
RSS_FEEDS = [
    ("https://news.opensuse.org/feed/", ""),  # No filter
    ("https://www.reddit.com/r/python.rss", "python")  # Filter for "python"
]
STORAGE_DIR = os.path.expanduser("~/reports/rss_monitor")
DB_PATH = os.path.join(STORAGE_DIR, "rss_monitor.db")
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465
SENDER_EMAIL = "your.email@gmail.com"
SENDER_PASSWORD = "your-app-specific-password"
RECEIVER_EMAIL = "your.email@gmail.com"
DEBUG = 0  # Set to 1 for debug output, 0 for silent (cron-friendly)
PRUNE_OLDER_THAN_DAYS = 30  # Prune articles older than this
ERROR_THRESHOLD = 3  # Email after this many consecutive failures
REQUEST_TIMEOUT = 10  # Timeout for fetching article content

def log(message):
    if DEBUG:
        print(message)

def ensure_storage_dir():
    os.makedirs(STORAGE_DIR, exist_ok=True)

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS feeds (
                feed_url TEXT PRIMARY KEY,
                last_checked TIMESTAMP,
                failure_count INTEGER DEFAULT 0
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                feed_url TEXT,
                identifier TEXT,
                title TEXT,
                link TEXT,
                description TEXT,
                fetched_content TEXT,
                pub_date TIMESTAMP,
                UNIQUE(feed_url, identifier)
            )
        """)
        conn.commit()

def hash_feed_url(url):
    return hashlib.sha256(url.encode()).hexdigest()[:16]

def get_article_identifier(item):
    if "guid" in item and item.guid:
        return item.guid
    if "link" in item and item.link:
        return item.link
    title = item.get("title", "")
    pubdate = item.get("published", item.get("pubdate", ""))
    return hashlib.sha256((title + pubdate).encode()).hexdigest()

def fetch_article_content(url):
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        doc = Document(response.text)
        content = doc.summary()
        # Strip HTML tags for plain text
        from html.parser import HTMLParser
        class MLStripper(HTMLParser):
            def __init__(self):
                super().__init__()
                self.reset()
                self.fed = []
            def handle_data(self, d):
                self.fed.append(d)
            def get_data(self):
                return ''.join(self.fed)
        stripper = MLStripper()
        stripper.feed(content)
        return stripper.get_data()
    except Exception as e:
        log(f"Failed to fetch content from {url}: {e}")
        return None

def matches_filter(item, fetched_content, filter_keywords):
    if not filter_keywords:
        return True
    filter_keywords = filter_keywords.lower()
    search_text = (
        item.get("title", "").lower() +
        item.get("description", "").lower() +
        (fetched_content or "").lower()
    )
    return filter_keywords in search_text

def prune_old_articles():
    cutoff = datetime.now() - timedelta(days=PRUNE_OLDER_THAN_DAYS)
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("DELETE FROM articles WHERE pub_date < ?", (cutoff,))
        conn.commit()
        log(f"Pruned articles older than {cutoff}")

def send_email(subject, body):
    msg = MIMEMultipart()
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECEIVER_EMAIL
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))
    
    context = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, msg.as_string())
        log(f"Email sent: {subject}")
    except Exception as e:
        log(f"Email sending failed: {e}")

def monitor_feeds():
    ensure_storage_dir()
    init_db()
    
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        for feed_url, _ in RSS_FEEDS:
            c.execute("INSERT OR IGNORE INTO feeds (feed_url, last_checked, failure_count) VALUES (?, ?, ?)",
                     (feed_url, datetime.now(), 0))
        conn.commit()
    
    for feed_url, filter_keywords in RSS_FEEDS:
        log(f"Checking feed: {feed_url} (filter: '{filter_keywords}')")
        feed = feedparser.parse(feed_url)
        
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            if feed.bozo:
                log(f"Error parsing feed {feed_url}: {feed.bozo_exception}")
                c.execute("UPDATE feeds SET failure_count = failure_count + 1 WHERE feed_url = ?", (feed_url,))
                c.execute("SELECT failure_count FROM feeds WHERE feed_url = ?", (feed_url,))
                failure_count = c.fetchone()[0]
                if failure_count >= ERROR_THRESHOLD:
                    subject = f"RSS Monitor Error: {feed_url}"
                    body = f"Failed to parse feed {feed_url} {failure_count} times. Error: {feed.bozo_exception}"
                    send_email(subject, body)
                conn.commit()
                continue
            else:
                c.execute("UPDATE feeds SET failure_count = 0, last_checked = ? WHERE feed_url = ?",
                         (datetime.now(), feed_url))
            
            c.execute("SELECT identifier FROM articles WHERE feed_url = ?", (feed_url,))
            seen = set(row[0] for row in c.fetchall())
            
            new_articles = []
            for item in feed.entries:
                identifier = get_article_identifier(item)
                if identifier not in seen:
                    title = item.get("title", "No title")
                    link = item.get("link", "No link")
                    description = item.get("description", "No description")
                    fetched_content = fetch_article_content(link) if link else None
                    pub_date = item.get("published", item.get("pubdate", datetime.now().isoformat()))
                    try:
                        pub_date = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
                    except:
                        pub_date = datetime.now()
                    
                    if matches_filter(item, fetched_content, filter_keywords):
                        new_articles.append((identifier, title, link, description, fetched_content, pub_date))
                        seen.add(identifier)
            
            if new_articles:
                feed_title = feed.feed.get("title", feed_url)
                body = f"New articles in {feed_title}:\n\n"
                for identifier, title, link, description, fetched_content, pub_date in new_articles:
                    body += f"Title: {title}\nLink: {link}\nContent: {fetched_content or description}\n\n"
                    c.execute("""
                        INSERT INTO articles (feed_url, identifier, title, link, description, fetched_content, pub_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (feed_url, identifier, title, link, description, fetched_content, pub_date))
                subject = f"New RSS Articles: {feed_title}"
                send_email(subject, body)
            
            conn.commit()
    
    prune_old_articles()

if __name__ == "__main__":
    monitor_feeds()
