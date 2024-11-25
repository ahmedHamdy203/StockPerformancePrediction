import asyncio
from tavily import TavilyClient
import requests
from bs4 import BeautifulSoup
import trafilatura
from typing import Dict, List, Set, Optional, Callable
import pandas as pd
from datetime import datetime, timedelta
import re
from urllib.parse import urlparse
import json
import os
from pathlib import Path
import logging
import time
from concurrent.futures import ThreadPoolExecutor
import hashlib
import aiohttp
from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_news_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RequestManager:
    """Handles HTTP requests with rate limiting and rotation"""
    
    def __init__(self):
        self.user_agent = UserAgent()
        self.session = aiohttp.ClientSession()
        self.domain_delays = {}
        
    def get_headers(self, domain: str) -> Dict[str, str]:
        """Generate headers with rotating User-Agent"""
        return {
            'User-Agent': self.user_agent.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        }
    
    async def close(self):
        """Close the aiohttp session"""
        if self.session and not self.session.closed:
            await self.session.close()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def get(self, url: str) -> Optional[str]:
        """Make HTTP GET request with rate limiting and retries"""
        domain = urlparse(url).netloc
        
        # Enforce domain-specific rate limiting
        if domain in self.domain_delays:
            await asyncio.sleep(self.domain_delays[domain])
        
        try:
            async with self.session.get(url, headers=self.get_headers(domain)) as response:
                if response.status == 200:
                    return await response.text()
                elif response.status in [403, 401]:
                    logger.warning(f"Access denied for {url} (status: {response.status})")
                    return None
                elif response.status == 503:
                    # Increase delay for this domain
                    self.domain_delays[domain] = self.domain_delays.get(domain, 1) * 2
                    logger.warning(f"Rate limited on {domain}, increasing delay to {self.domain_delays[domain]}s")
                    raise Exception(f"Rate limited: {response.status}")
                else:
                    logger.error(f"Unexpected status {response.status} for {url}")
                    return None
                    
        except Exception as e:
            logger.error(f"Request failed for {url}: {str(e)}")
            raise

class NewsSourceHandler:
    """Handles different news sources with specific extraction logic"""
    
    def __init__(self, request_manager: RequestManager):
        self.request_manager = request_manager
        self.handlers = {
            'finance.yahoo.com': self.handle_yahoo,
            'seekingalpha.com': self.handle_seeking_alpha,
            'bloomberg.com': self.handle_bloomberg,
            'wsj.com': self.handle_wsj,
            'reuters.com': self.handle_reuters
        }
    
    def get_handler(self, url: str) -> Callable:
        """Get appropriate handler for the domain"""
        domain = urlparse(url).netloc
        for known_domain, handler in self.handlers.items():
            if known_domain in domain:
                return handler
        return self.handle_default
    
    async def handle_yahoo(self, url: str) -> Optional[str]:
        """Handle Yahoo Finance specific extraction"""
        # Add delay for Yahoo Finance due to aggressive rate limiting
        await asyncio.sleep(2)
        content = await self.request_manager.get(url)
        if content:
            soup = BeautifulSoup(content, 'html.parser')
            article = soup.find('div', {'class': 'caas-body'})
            return article.get_text() if article else None
        return None
    
    async def handle_seeking_alpha(self, url: str) -> Optional[str]:
        """Handle SeekingAlpha specific extraction"""
        # SeekingAlpha requires subscription, try alternative free content
        if '/article/' in url:
            summary_url = url.replace('/article/', '/summary/')
            content = await self.request_manager.get(summary_url)
            if content:
                soup = BeautifulSoup(content, 'html.parser')
                summary = soup.find('div', {'class': 'summary'})
                return summary.get_text() if summary else None
        return None
    
    async def handle_bloomberg(self, url: str) -> Optional[str]:
        """Handle Bloomberg specific extraction"""
        # Bloomberg has strict anti-bot measures, use minimal parsing
        content = await self.request_manager.get(url)
        if content:
            soup = BeautifulSoup(content, 'html.parser')
            article = soup.find('div', {'class': 'body-content'})
            return article.get_text() if article else None
        return None
    
    async def handle_wsj(self, url: str) -> Optional[str]:
        """Handle Wall Street Journal specific extraction"""
        # WSJ requires subscription, try to get preview content
        content = await self.request_manager.get(url)
        if content:
            soup = BeautifulSoup(content, 'html.parser')
            preview = soup.find('div', {'class': 'article-preview'})
            return preview.get_text() if preview else None
        return None
    
    async def handle_reuters(self, url: str) -> Optional[str]:
        """Handle Reuters specific extraction"""
        content = await self.request_manager.get(url)
        if content:
            soup = BeautifulSoup(content, 'html.parser')
            article = soup.find('div', {'class': 'article-body'})
            return article.get_text() if article else None
        return None
    
    async def handle_default(self, url: str) -> Optional[str]:
        """Default handler for unknown sources"""
        # Try trafilatura first
        try:
            downloaded = await asyncio.to_thread(trafilatura.fetch_url, url)
            if downloaded:
                content = await asyncio.to_thread(trafilatura.extract, downloaded)
                if content:
                    return content
        except Exception as e:
            logger.error(f"Trafilatura extraction failed: {str(e)}")
        
        # Fallback to BeautifulSoup
        try:
            content = await self.request_manager.get(url)
            if content:
                soup = BeautifulSoup(content, 'html.parser')
                
                # Remove unwanted elements
                for element in soup(['script', 'style', 'nav', 'header', 'footer', 'ads']):
                    element.decompose()
                
                content = soup.get_text(separator='\n')
                lines = [line.strip() for line in content.split('\n') if line.strip()]
                return '\n'.join(lines)
        except Exception as e:
            logger.error(f"BeautifulSoup extraction failed: {str(e)}")
        
        return None

class EnhancedStockNewsCollector:
    """Main class for collecting and analyzing stock news"""

    def __init__(self, api_key: str, storage_dir: str = "stock_news_data"):
        self.client = TavilyClient(api_key=api_key)
        self.storage_dir = Path(storage_dir)
        self.request_manager = RequestManager()
        self.news_handler = NewsSourceHandler(self.request_manager)
        self.relevance_threshold = 0.
        self._setup_storage()
        
        # Keywords initialization remains the same as in your original code
        self.breaking_news_keywords = {
            'urgent', 'breaking', 'alert', 'just in', 'exclusive',
            'update', 'developing', 'news flash', 'breaking news'
        }
        
        self.stock_event_keywords = {
            'earnings', 'guidance', 'acquisition', 'merger', 'buyout',
            'sec filing', 'investigation', 'lawsuit', 'ceo', 'executive',
            'dividend', 'stock split', 'delisting', 'ipo', 'offering',
            'partnership', 'contract', 'patent', 'fda', 'approval',
            'restructuring', 'layoffs', 'bankruptcy', 'default',
            'profit warning', 'downgrade', 'upgrade', 'price target'
        }
        
        self.financial_keywords = {
            'revenue', 'profit', 'loss', 'margin', 'eps', 'income',
            'forecast', 'outlook', 'guidance', 'quarter', 'annual',
            'beat', 'miss', 'estimate', 'consensus'
        }

    def _setup_storage(self):
        """Initialize storage directory structure"""
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        (self.storage_dir / "daily").mkdir(exist_ok=True)
        (self.storage_dir / "archive").mkdir(exist_ok=True)

    def _get_storage_path(self, ticker: str, date_str: str = None) -> Path:
        """Generate storage path for a specific ticker and date"""
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        return self.storage_dir / "daily" / f"{ticker}_{date_str}.json"

    def generate_search_queries(self, ticker: str, company_name: str) -> List[str]:
        """Generate multiple targeted search queries"""
        current_date = datetime.now().strftime("%Y")
        return [
            f'breaking news {company_name} OR ${ticker} stock',
            f'urgent {company_name} OR ${ticker} stock news',
            f'{company_name} OR ${ticker} earnings financial results {current_date}',
            f'{company_name} OR ${ticker} guidance forecast outlook',
            f'{company_name} OR ${ticker} merger acquisition partnership announcement',
            f'{company_name} OR ${ticker} management changes executive',
            f'{company_name} OR ${ticker} SEC filing investigation regulatory',
            f'{company_name} OR ${ticker} stock price movement significant',
            f'{company_name} OR ${ticker} analyst rating upgrade downgrade'
        ]

    def calculate_relevance_score(self, article: Dict) -> float:
        """Calculate custom relevance score based on multiple factors"""
        score = 0.0
        text_to_analyze = f"{article.get('title', '')} {article.get('snippet', '')}".lower()
        
        # Check for breaking news indicators
        if any(keyword in text_to_analyze for keyword in self.breaking_news_keywords):
            score += 0.3
            
        # Check for stock-specific events
        if any(keyword in text_to_analyze for keyword in self.stock_event_keywords):
            score += 0.3
            
        # Check for financial metrics
        if any(keyword in text_to_analyze for keyword in self.financial_keywords):
            score += 0.2
            
        # Consider article freshness
        try:
            pub_date = pd.to_datetime(article.get('date_published'))
            hours_old = (datetime.now() - pub_date).total_seconds() / 3600
            freshness_score = max(0, 0.2 * (1 - (hours_old / 72)))
            score += freshness_score
        except:
            pass
            
        return min(1.0, score + article.get('score', 0) * 0.3)

    def save_articles(self, ticker: str, articles: List[Dict]) -> None:
        """Save articles to JSON file with date organization"""
        if not articles:
            return

        # Group articles by date
        articles_by_date = {}
        for article in articles:
            try:
                date_str = pd.to_datetime(article['publish_date']).strftime("%Y-%m-%d")
            except:
                date_str = datetime.now().strftime("%Y-%m-%d")
            
            if date_str not in articles_by_date:
                articles_by_date[date_str] = []
            articles_by_date[date_str].append(article)

        # Save articles for each date
        for date_str, dated_articles in articles_by_date.items():
            file_path = self._get_storage_path(ticker, date_str)
            
            # Load existing articles
            existing_articles = []
            if file_path.exists():
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        existing_articles = json.load(f)
                except json.JSONDecodeError:
                    logger.error(f"Error reading existing file: {file_path}")

            # Combine and deduplicate articles
            all_articles = existing_articles + dated_articles
            unique_articles = []
            seen_urls = set()
            
            for article in all_articles:
                if article['url'] not in seen_urls:
                    seen_urls.add(article['url'])
                    unique_articles.append(article)

            # Save to file
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(unique_articles, f, indent=2, ensure_ascii=False)
                logger.info(f"Saved {len(dated_articles)} articles for {ticker} on {date_str}")
            except Exception as e:
                logger.error(f"Error saving articles: {str(e)}")

    async def fetch_company_news(
        self, 
        ticker: str, 
        company_name: str,
        hours_back: int = 72,
        max_results: int = 30
    ) -> List[Dict]:
        """Fetch and process company news articles"""
        all_articles = []
        seen_urls = set()
        
        # Generate search queries
        queries = self.generate_search_queries(ticker, company_name)
        
        for query in queries:
            try:
                response = await asyncio.to_thread(
                    self.client.search,
                    query=query,
                    search_depth="advanced",
                    include_domains=[
                        "reuters.com", "bloomberg.com", "cnbc.com", "wsj.com", 
                        "ft.com", "seekingalpha.com", "fool.com", "marketwatch.com",
                        "investing.com", "finance.yahoo.com", "barrons.com"
                    ],
                    exclude_domains=["reddit.com", "twitter.com"],
                    max_results=max_results,
                    search_type="news"
                )
                
                for article in response.get('results', []):
                    url = article.get('url', '')
                    if url not in seen_urls:
                        seen_urls.add(url)
                        
                        # Get content using appropriate handler
                        handler = self.news_handler.get_handler(url)
                        content_info = {'full_content': '', 'word_count': 0, 'extraction_method': 'none'}
                        
                        try:
                            content = await handler(url)
                            if content:
                                content_info = {
                                    'full_content': content,
                                    'word_count': len(content.split()),
                                    'extraction_method': handler.__name__
                                }
                        except Exception as e:
                            logger.error(f"Content extraction failed for {url}: {str(e)}")
                        
                        # Calculate custom relevance score
                        custom_score = self.calculate_relevance_score(article)
                        
                        # Process article if it meets criteria
                        if custom_score >= self.relevance_threshold and content_info['word_count'] > 100:
                            processed_article = {
                                'title': article.get('title', ''),
                                'summary': article.get('snippet', ''),
                                'url': url,
                                'source': article.get('domain', ''),
                                'original_score': article.get('score', 0.0),
                                'custom_score': custom_score,
                                'publish_date': article.get('date_published', ''),
                                'ticker': ticker,
                                'company_name': company_name,
                                'collected_at': datetime.now().isoformat(),
                                'full_content': content_info['full_content'],
                                'word_count': content_info['word_count'],
                                'extraction_method': content_info['extraction_method']
                            }
                            all_articles.append(processed_article)
                            
                # Add delay between queries to avoid rate limiting
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"Error fetching news for query '{query}': {str(e)}")
                continue
        
        # Save articles to storage
        self.save_articles(ticker, all_articles)
        
        # Sort by custom score and return top results
        return sorted(all_articles, key=lambda x: x['custom_score'], reverse=True)

    def load_articles(self, ticker: str, days_back: int = 7) -> List[Dict]:
        """Load articles from storage for a given period"""
        all_articles = []
        current_date = datetime.now()

        for i in range(days_back):
            date = current_date - timedelta(days=i)
            date_str = date.strftime("%Y-%m-%d")
            file_path = self._get_storage_path(ticker, date_str)
            
            if file_path.exists():
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        articles = json.load(f)
                        all_articles.extend(articles)
                except json.JSONDecodeError as e:
                    logger.error(f"Error loading articles from {file_path}: {e}")

        return all_articles

    async def close(self):
        """Cleanup resources"""
        await self.request_manager.close()

class NewsCollectorMetrics:
    """Track and report collector performance metrics"""
    
    def __init__(self):
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.extraction_methods = {}
        self.start_time = datetime.now()
    
    def log_request(self, success: bool, extraction_method: str = None):
        """Log request outcome"""
        self.total_requests += 1
        if success:
            self.successful_requests += 1
            if extraction_method:
                self.extraction_methods[extraction_method] = self.extraction_methods.get(extraction_method, 0) + 1
        else:
            self.failed_requests += 1
    
    def get_report(self) -> Dict:
        """Generate performance report"""
        runtime = (datetime.now() - self.start_time).total_seconds()
        return {
            'runtime_seconds': runtime,
            'total_requests': self.total_requests,
            'successful_requests': self.successful_requests,
            'failed_requests': self.failed_requests,
            'success_rate': self.successful_requests / max(self.total_requests, 1),
            'requests_per_second': self.total_requests / max(runtime, 1),
            'extraction_methods': self.extraction_methods
        }

async def main():
    """Example usage of the enhanced news collector"""
    collector = EnhancedStockNewsCollector(
        api_key="your-tavily-api-key",
        storage_dir="stock_news_data"
    )
    
    metrics = NewsCollectorMetrics()
    
    try:
        logger.info("Fetching new articles...")
        articles = await collector.fetch_company_news(
            ticker="AAPL",
            company_name="Apple Inc",
            hours_back=72,
            max_results=30
        )
        
        # Print results
        logger.info(f"\nFound {len(articles)} relevant articles:")
        for article in articles:
            print(f"\nTitle: {article['title']}")
            print(f"Source: {article['source']}")
            print(f"Score: {article['custom_score']:.2f}")
            print(f"Published: {article['publish_date']}")
            print(f"URL: {article['url']}")
            print(f"Extraction Method: {article['extraction_method']}")
            print("\nSummary:")
            print(article['summary'])
            print("-" * 80)
            
        # Load recent articles from storage
        stored_articles = collector.load_articles("AAPL", days_back=7)
        logger.info(f"Loaded {len(stored_articles)} articles from storage")
        
        # Print metrics
        metrics_report = metrics.get_report()
        logger.info("Collection Metrics:")
        logger.info(json.dumps(metrics_report, indent=2))
            
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise
    finally:
        await collector.close()

if __name__ == "__main__":
    asyncio.run(main())
                    