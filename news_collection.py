import requests
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Function to get news articles from NewsAPI
def fetch_news(query, api_key, from_date, to_date, page_size=20):
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query,
        "from": from_date,
        "to": to_date,
        "apiKey": api_key,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": page_size
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        return response.json().get("articles", [])
    else:
        print("Error fetching news:", response.status_code, response.text)
        return []

# Save news articles to a JSON file
def save_news_to_file(ticker, articles, date):
    filename = f"{ticker}_news_{date}.json"
    with open(filename, "w") as file:
        json.dump(articles, file, indent=4)
    print(f"News articles saved to {filename}")

# Main program
if __name__ == "__main__":
    # Your NewsAPI key loaded from .env file
    API_KEY = os.getenv("NEWSAPI_KEY")

    if not API_KEY:
        print("Error: NEWSAPI_KEY is not set in the .env file.")
        exit(1)

    # User inputs the stock ticker
    ticker = input("Enter the stock ticker: ").strip()
    search_query = ticker

    # Date range for the news
    today = datetime.now()
    five_days_ago = today - timedelta(days=5)

    from_date = five_days_ago.strftime("%Y-%m-%d")
    to_date = today.strftime("%Y-%m-%d")

    print(f"Fetching news for the stock: '{ticker}' from {from_date} to {to_date}...")

    # Fetch news articles
    articles = fetch_news(search_query, API_KEY, from_date, to_date)

    # Add metadata to articles
    articles_with_metadata = [{
        "title": article.get("title"),
        "source": article.get("source", {}).get("name"),
        "publishedAt": article.get("publishedAt"),
        "url": article.get("url")
    } for article in articles]

    # Save the news to a JSON file
    date_str = today.strftime("%Y-%m-%d")
    save_news_to_file(ticker, articles_with_metadata, date_str)

    # Display the articles
    for i, article in enumerate(articles_with_metadata):
        print(f"{i + 1}. {article['title']}")
        print(f"   Source: {article['source']}")
        print(f"   Published At: {article['publishedAt']}")
        print(f"   URL: {article['url']}\n")
