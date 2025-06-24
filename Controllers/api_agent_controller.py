from fastapi import APIRouter, Query
import Agents.ApiAgent.api_agent_base_functions as agbf
from fastapi_cache.decorator import cache

router = APIRouter()

@router.get('/GetLatestNews')
@cache(expire=86400)  
async def get_latest_news(no_of_news_items: int):
    return await agbf.get_market_news_and_sentiment_latest(no_of_news_items)

@router.get("/GetIncomeStatement")
@cache(expire=86400)  
async def get_income_statements(company_name: str, no_of_income_statements: int = 1):
    return await agbf.get_latest_annual_income_statements(company_name, no_of_income_statements)

@router.get("/GetLatestEPS")
@cache(expire=86400)  
async def fetch_latest_eps(company_name: str):
    return await agbf.get_latest_eps(company_name)