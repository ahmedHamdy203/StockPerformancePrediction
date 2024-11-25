# Enhanced Stock News Collector 📈

A powerful Python tool for collecting and analyzing breaking stock market news with smart relevance scoring and persistent storage.

## Quick Start 🚀

```python
import asyncio
from stock_news_collector import EnhancedStockNewsCollector

async def main():
    # Initialize collector
    collector = EnhancedStockNewsCollector(api_key="your-tavily-api-key")
    
    # Fetch news
    articles = await collector.fetch_company_news(
        ticker="AAPL",
        company_name="Apple Inc"
    )
    
    # Print results
    for article in articles:
        print(f"Title: {article['title']}")
        print(f"Score: {article['custom_score']}")
        print(f"URL: {article['url']}\n")

if __name__ == "__main__":
    asyncio.run(main())
```

## Configuration ⚙️

```python
collector = EnhancedStockNewsCollector(
    api_key="your-tavily-api-key",
    storage_dir="stock_news_data",  # Optional: custom storage location
)
```

## Main Functions 🔧

```python
# Fetch breaking news
articles = await collector.fetch_company_news(
    ticker="AAPL",
    company_name="Apple Inc",
    hours_back=72,      # Optional: time window
    max_results=30      # Optional: results per query
)

# Load stored articles
stored_articles = collector.load_articles(
    ticker="AAPL",
    days_back=7        # Optional: historical window
)
```

## Article Data Structure 📝

```python
{
    'title': str,            # Article title
    'summary': str,          # Brief summary
    'url': str,             # Source URL
    'source': str,          # Domain name
    'custom_score': float,   # Relevance score
    'publish_date': str,     # Publication date
    'full_content': str,     # Complete article text
}
```

## Built With 🏗️

- [Tavily API](https://tavily.com/) - News search
- BeautifulSoup4 - Content parsing
- Trafilatura - Text extraction
- Pandas - Data handling
- AsyncIO - Asynchronous operations

