import json
import aiohttp
import Agents.ApiAgent.api_agent_base_variables as base_variables
import Agents.ApiAgent.alpha_vantage_retreival_variables as retrieval_var

def pretty_print(data: dict):
    print(json.dumps(data, indent=3))
    
    
def build_url(function: str, symbol: str)->str:
    url = (
        f"{base_variables.alpha_vantage_base_url}"
        f"{base_variables.alpha_vantage_query_url}"
        f"{function}&symbol={symbol}&apikey={base_variables.alpha_vantage_api_key}"
    )
    return url
        
async def retrieve_data(url: str) -> dict:
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        async with session.get(url) as response:
            return await response.json()

async def get_balance_sheet_annual_report(symbol: str, no_of_years: int = 0) -> dict:
    """
    Retrieves the nth latest annual balance sheet report for the given stock symbol.

    Parameters:
    symbol (str): The stock ticker symbol for the company (e.g., "AAPL").
    no_of_years (int, optional): Index of the annual report to retrieve, 
        where 0 is the latest, 1 is the second latest, and so on. Default is 1 (second latest).

    Returns:
    dict: A dictionary containing the specified annual balance sheet data.

    Raises:
    ValueError: If no_of_years is negative or if there are fewer reports available than requested.
    """
    if no_of_years < 0:
        raise ValueError("no_of_years must be 0 or greater")
    
    url = build_url(retrieval_var.BALANCE_SHEET, symbol)
    response = await retrieve_data(url)
    reports = response.get('annualReports', [])
    
    if len(reports) <= no_of_years:
        raise ValueError(f"Only {len(reports)} balance sheet report(s) available for {symbol}")
    
    return reports[no_of_years]



async def get_latest_annual_income_statements(symbol: str, no_of_income_statements: int = 1) -> list[dict]:
    """
        Retrieves the latest N annual income statements for the given stock symbol.

        Parameters:
        - symbol: Stock ticker symbol.
        - no_of_income_statements: Number of most recent annual income statements to retrieve (default is 1).

        Returns:
        - A list of dictionaries, each representing an annual income statement.

        Raises:
        - ValueError if no_of_income_statements is less than 1 or if the reports are unavailable.
    """

    no_of_income_statements = no_of_income_statements - 1
    if no_of_income_statements < 0:
        raise ValueError(f"Please enter no of income statements >=1")

    url = build_url(retrieval_var.INCOME_STATEMENT, symbol)
    print(url)
    response = await retrieve_data(url)
    reports = response.get('annualReports', [])
    print(reports)
    if not reports:
        raise ValueError(f"No income statement reports available for symbol '{symbol}'")

    if no_of_income_statements >= len(reports):
        raise ValueError(f"Requested report index {no_of_income_statements} is out of range. "
                         f"Only {len(reports)} reports available for {symbol}")

    return reports[:no_of_income_statements]

async def get_market_news_and_sentiment_latest(no_of_news_items: int = 80) -> dict:
    """
    Retrieves the latest market news and sentiment data.
    sorted based on latest news can also set sort=RELEVANCE
    Parameters:
        api_key (str): Your Alpha Vantage API key.
        no_of_news_items (int): Number of news items to retrieve (max 50).

    Returns:
        dict: Parsed JSON response containing news and sentiment data.
    """
    base_url = base_variables.alpha_vantage_base_url
    query_url = base_variables.alpha_vantage_query_url
    url = f'{base_url}{query_url}{retrieval_var.NEWS_SENTIMENT}&items={no_of_news_items}&apikey={base_variables.alpha_vantage_api_key}'
    print(url)
    response = await retrieve_data(url)
    return response

async def get_latest_eps(symbol: str):
    url = build_url(retrieval_var.EARNINGS, symbol)
    response = await retrieve_data(url)
    report = response.get('annualEarnings', 0)
    return report[0]

