from Common.common_functions import read_json
import Agents.ApiAgent.api_agent_base_variables as base_variables_api
import Agents.ScrapingAgent.scraping_base_variables as base_variables_scraping
import DBO.database_variables as db_var

def set_base_variables():
    path = 'EnvironmentSettings/appsettings.json'
    json_data = read_json(path)
    set_db_var(json_data)
    set_alphavantage_var(json_data)
    set_yahoofinance_var(json_data)
    set_sec_edgar_var(json_data)
    


def set_alphavantage_var(json_data):
    base_variables_api.alpha_vantage_api_key = json_data['AlphaVantage_api_key']
    base_variables_api.alpha_vantage_base_url = json_data['AlphaVantage_Base_URL']
    base_variables_api.alpha_vantage_query_url = json_data['AlphaVantage_Query_Url']

def set_yahoofinance_var(json_data):
    base_variables_scraping.yahoo_base_url = json_data['YahooFinance_Base_URL']
    base_variables_scraping.yahoo_earnings_endpoint = json_data['YahooFinance_Earnings_URL']

def set_sec_edgar_var(json_data):
    base_variables_scraping.sec_edgar_base_url = json_data['SEC_EDGAR_Base_URL']
    base_variables_scraping.sec_edgar_search_endpoint = json_data['SEC_EDGAR_SEARCH_ENDPOINT']
    base_variables_scraping.sec_edgar_complete_search_url = json_data['SEC_EDGAR_COMPLETE_SEARCH_URL']

def set_db_var(json_data):
    db_var.server = json_data['Database']['Server']
    db_var.database = json_data['Database']['Database']
    db_var.username = json_data['Database']['Username']
    db_var.password = json_data['Database']['Password']
    db_var.driver = json_data['Database']['Driver']
    db_var.trust_cert = json_data['Database']['TrustServerCertificate']