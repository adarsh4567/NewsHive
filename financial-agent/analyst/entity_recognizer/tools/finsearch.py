from langchain_core.tools import tool
from typing import List
from dotenv import load_dotenv
import asyncio
import aiohttp

load_dotenv()

categories = ["INCOME_STATEMENT","OVERVIEW","GLOBAL_QUOTE","CASH_FLOW"]

async def fetch_category_async(session,category,stock):
    try:
        async with session.get(f"https://www.alphavantage.co/query?function={category}&symbol={stock}&apikey=ZM4GXRDR29MSZEDN",timeout=5) as response:
            response.raise_for_status()
            # Return stock ticker as well so we can route it later
            return stock, category, await response.json()
    except aiohttp.ClientError as e:
        return stock, category, f"Error: {e}"    

@tool
async def stock_tool(tickers:List[str]) -> dict:
    """
    Return a dict of stock metrics for the available tickers.
    """
    if not tickers:
        return {
            "stockReport":{}
        }
    
    final_dict={ticker: {} for ticker in tickers}

    async with aiohttp.ClientSession() as session:
        results = []
        for x in tickers:
            for category in categories:
                res = await fetch_category_async(session,category,x)
                results.append(res)
                # Sleep to prevent hitting the free-tier rate limit (5 calls/min)
                await asyncio.sleep(1)
        
        for stock, category, result in results:
            ticker_data = final_dict[stock]
            # Add safety check in case API fails or returns error string
            if isinstance(result, str) or "annualReports" not in result and category in ["INCOME_STATEMENT", "CASH_FLOW"]:
                 continue
            if "Global Quote" not in result and category == "GLOBAL_QUOTE":
                 continue
                 
            try:
                match category:
                    case "INCOME_STATEMENT":
                        ticker_data["NetIncome"] = result["annualReports"][0]["netIncome"]
                        ticker_data["GrossProfit"] = result["annualReports"][0]["grossProfit"]
                    case "OVERVIEW":
                        ticker_data["PERatio"] = result["PERatio"]
                        ticker_data["PEGRatio"] = result["PEGRatio"]
                        ticker_data["EPS"] = result["EPS"]
                        ticker_data["ProfitMargin"] = result["ProfitMargin"]  
                        ticker_data["ReturnOnEquityTTM"] = result["ReturnOnEquityTTM"]
                        ticker_data["RevenueTTM"] = result["RevenueTTM"]
                        ticker_data["GrossProfitTTM"] = result["GrossProfitTTM"]
                        ticker_data["GrossProfitMarginTTM"] = (int(result["GrossProfitTTM"])/int(result["RevenueTTM"]))*100
                        ticker_data["PriceToBookRatio"] = result["PriceToBookRatio"]
                        ticker_data["PriceToSalesRatioTTM"] = result["PriceToSalesRatioTTM"]
                        ticker_data["ReturnOnEquityTTM"] = result["ReturnOnEquityTTM"]
                    case "CASH_FLOW":
                        ticker_data["OperatingCashflow"] = result["annualReports"][0]["operatingCashflow"]
                    case "GLOBAL_QUOTE":
                        ticker_data["CurrentPrice"] = result["Global Quote"]["05. price"]
            except (KeyError, IndexError, ValueError):
                # Safely ignore missing data for a specific field/ticker
                pass

    return {
        "stockReport": final_dict
    }                         
                
