from typing import List
import yfinance as yf
import asyncio


def _get_news_sync(ticker: str):
    # This runs completely synchronously in a background thread
    return yf.Ticker(ticker).get_news(count=3)

async def fetch_news(x):
    # Pass the function reference and its arguments to to_thread
    return await asyncio.to_thread(_get_news_sync, x)


async def recent_news(symbols: List[str]) -> dict:
    """
    return recent news for symbols
    """
    # 1. Fire off all network requests at the exact same time
    tasks = [fetch_news(x) for x in symbols]
    
    # 2. Wait for all of them to complete concurrently
    results = await asyncio.gather(*tasks)
    
    # 3. Process the results
    final_news = {}
    for ticker, news_items in zip(symbols, results):
        summaries = [n["content"]["summary"] for n in news_items]
        final_news[ticker] = summaries

    return {"recentnews": final_news}




